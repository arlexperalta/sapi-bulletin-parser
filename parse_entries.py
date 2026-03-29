#!/usr/bin/env python3
"""
Extractor IA — Paso 2 del nuevo pipeline.

Lee el texto extraído por pymupdf (Fase 1) y genera JSON estructurado.
La lógica está diseñada a partir de la lectura directa del B651 PDF.

Soporta:
  - Formato A: Inscripción (Publicación Prensa, Concedidas, Observadas sin lugar)
  - Formato A+: Negadas Art.27 (+ COMENTARIO)
  - Formato A++: Negadas Art.33, Observadas con lugar (+ REGISTROS NEGANTES)

Uso:
  python3 extractor/extraer_ia.py extracciones/B651/texto/b651_tomo1_completo.txt
"""

import sys
import re
import json
from pathlib import Path


# Líneas de ruido que hay que ignorar (paginación, headers)
NOISE_PATTERNS = [
    re.compile(r'^\d+$'),  # números de página sueltos
    re.compile(r'^Tomo [IVXLC]+\s*$'),  # "Tomo I", "Tomo IX"
    re.compile(r'^Boletín de la Propiedad Industrial'),
    re.compile(r'^No\.\s*\d+\s*$'),
    re.compile(r'^\s*$'),  # líneas vacías
]

def es_ruido(linea: str) -> bool:
    """Detecta líneas de ruido (paginación, headers)."""
    s = linea.strip()
    if not s:
        return True
    for p in NOISE_PATTERNS:
        if p.match(s):
            return True
    return False


def limpiar_bloque(lineas: list[str]) -> list[str]:
    """Elimina líneas de ruido de un bloque de entrada."""
    resultado = []
    for linea in lineas:
        if not es_ruido(linea):
            resultado.append(linea)
    return resultado


def extraer_fecha_inscripcion(texto_insc: str) -> str | None:
    """Extrae fecha de 'Insc. YYYY-NNNNNN del DD DE MES DE YYYY'."""
    meses = {
        'ENERO': '01', 'FEBRERO': '02', 'MARZO': '03', 'ABRIL': '04',
        'MAYO': '05', 'JUNIO': '06', 'JULIO': '07', 'AGOSTO': '08',
        'SEPTIEMBRE': '09', 'OCTUBRE': '10', 'NOVIEMBRE': '11', 'DICIEMBRE': '12'
    }
    m = re.search(r'del\s+(\d{1,2})\s+DE\s+(\w+)\s+DE\s+(\d{4})', texto_insc, re.IGNORECASE)
    if m:
        dia = m.group(1).zfill(2)
        mes = meses.get(m.group(2).upper(), '00')
        año = m.group(3)
        return f"{año}-{mes}-{dia}"
    return None


def extraer_nro_solicitud(texto_insc: str) -> str | None:
    """Extrae número de solicitud de 'Insc. YYYY-NNNNNN' o 'Insc. YY-NNNNNN'."""
    m = re.search(r'Insc\.\s*(\d{2,4}[-‐]\d{5,6})', texto_insc)
    if m:
        return m.group(1).replace('‐', '-')  # normalizar guión
    return None


def parsear_titular(texto: str) -> dict:
    """Parsea titular en dos formatos:
    B651+: 'NOMBRE Domicilio: DOM País: PAIS'
    B530:  'NOMBRE. Nacionalidad: PAIS. Domicilio: DOM'
    """
    resultado = {'nombre': '', 'domicilio': '', 'pais': ''}

    # Detectar formato B530: Nacionalidad antes de Domicilio
    m_nac = re.search(r'Nacionalidad:\s*(.+?)\.\s*Domicilio:', texto)
    if m_nac:
        resultado['pais'] = m_nac.group(1).strip()
        resultado['nombre'] = texto[:re.search(r'\.?\s*Nacionalidad:', texto).start()].strip().rstrip('.')
        m_dom = re.search(r'Domicilio:\s*(.+)$', texto)
        if m_dom:
            resultado['domicilio'] = m_dom.group(1).strip().rstrip('.')
        return resultado

    # Formato B651+: País al final
    m_pais = re.search(r'País:\s*(.+)$', texto, re.IGNORECASE)
    if m_pais:
        resultado['pais'] = m_pais.group(1).strip()
        texto_sin_pais = texto[:m_pais.start()].strip()
    else:
        texto_sin_pais = texto.strip()

    # Buscar Domicilio:
    m_dom = re.search(r'Domicilio:\s*(.+)$', texto_sin_pais, re.IGNORECASE)
    if m_dom:
        resultado['domicilio'] = m_dom.group(1).strip().rstrip('.')
        resultado['nombre'] = texto_sin_pais[:m_dom.start()].strip()
    else:
        resultado['nombre'] = texto_sin_pais.strip()

    return resultado


def parsear_bloque_inscripcion(lineas: list[str]) -> dict | None:
    """Parsea un bloque de formato A (inscripción)."""
    if not lineas:
        return None

    entrada = {
        'nroSolicitud': None,
        'fechaInscripcion': None,
        'nombreMarca': None,
        'titularNombre': None,
        'titularDomicilio': None,
        'titularPais': None,
        'nombre': None,
        'clase': None,
        'distingue': None,
        'descripcionEtiqueta': None,
        'tramitante': None,
        'comentario': None,
        'registrosNegantes': None,
    }

    # Unir todo en un texto para buscar campos
    texto_completo = '\n'.join(lineas)

    # 1. Buscar Insc.
    m_insc = re.search(r'Insc\.\s*(\d{2,4}[-‐]\d{5,6})\s+del\s+(.+?)(?=\n|SOLICITADA|NOMBRE DE LA MARCA)', texto_completo, re.DOTALL)
    if m_insc:
        entrada['nroSolicitud'] = m_insc.group(1).replace('‐', '-')
        entrada['fechaInscripcion'] = extraer_fecha_inscripcion(m_insc.group(0))
    else:
        # Sin número de solicitud, no es una entrada válida
        return None

    # 2. NOMBRE DE LA MARCA (opcional, antes de SOLICITADA POR)
    m_nombre_marca = re.search(r'NOMBRE DE LA MARCA:\s*(.+?)(?=\n)', texto_completo)
    if m_nombre_marca:
        entrada['nombreMarca'] = m_nombre_marca.group(1).strip()
        entrada['nombre'] = entrada['nombreMarca']

    # 3. SOLICITADA POR — extraer titular y nombre de marca
    m_sol = re.search(r'SOLICITADA POR:\s*', texto_completo)
    if m_sol:
        resto = texto_completo[m_sol.end():]

        # El bloque titular+nombre termina en "EN CLASE:" o "PARA DISTINGUIR:" (formato viejo)
        m_fin = re.search(r'\n(?:EN CLASE:|PARA DISTINGUIR:)', resto)
        if m_fin:
            bloque_titular_nombre = resto[:m_fin.start()]
        else:
            bloque_titular_nombre = resto

        lineas_bloque = bloque_titular_nombre.strip().split('\n')
        lineas_bloque = [l for l in lineas_bloque if not es_ruido(l)]

        # Unir líneas para manejar país partido en múltiples líneas
        # El país puede partirse en 2-3 líneas: "País: ESTADOS UNIDOS DE\nAMÉRICA"
        # Después del país completo viene el nombre de marca (o nada si es figurativa)
        #
        # Estrategia: acumular líneas después de "País:" hasta que el texto
        # forme un país conocido, o hasta que la siguiente línea sea un keyword.

        PAISES_CONOCIDOS = {
            'VENEZUELA', 'ESTADOS UNIDOS DE AMÉRICA', 'ESTADOS UNIDOS DE AMERICA',
            'COLOMBIA', 'ARGENTINA', 'BRASIL', 'MÉXICO', 'MEXICO', 'CHILE', 'PERÚ', 'PERU',
            'ESPAÑA', 'FRANCIA', 'ALEMANIA', 'ITALIA', 'SUIZA', 'REINO UNIDO',
            'CHINA', 'JAPÓN', 'JAPON', 'INDIA', 'COREA', 'COREA DEL SUR',
            'PAISES BAJOS', 'PAÍSES BAJOS', 'HOLANDA', 'LUXEMBURGO', 'BÉLGICA', 'BELGICA',
            'SUECIA', 'DINAMARCA', 'NORUEGA', 'FINLANDIA', 'AUSTRIA',
            'CANADÁ', 'CANADA', 'AUSTRALIA', 'NUEVA ZELANDA',
            'ISLAS VÍRGENES BRITÁNICAS', 'ISLAS VIRGENES BRITANICAS',
            'ISLAS VÍRGENES', 'BERMUDA', 'PANAMÁ', 'PANAMA',
            'TURQUÍA', 'TURQUIA', 'RUSIA', 'IRLANDA',
            'ANTILLAS HOLANDESAS', 'ANTILLAS NEERLANDESAS',
            'PUERTO RICO', 'ECUADOR', 'URUGUAY', 'PARAGUAY', 'BOLIVIA',
            'COSTA RICA', 'GUATEMALA', 'CUBA', 'JAMAICA', 'TRINIDAD Y TOBAGO',
            'MALASIA', 'TAILANDIA', 'FILIPINAS', 'INDONESIA', 'SINGAPUR',
            'SUDÁFRICA', 'NIGERIA', 'MARRUECOS', 'EGIPTO',
            'ISRAEL', 'EMIRATOS ÁRABES UNIDOS', 'ARABIA SAUDITA',
        }

        lineas_unidas = []
        i = 0
        while i < len(lineas_bloque):
            linea = lineas_bloque[i].rstrip()

            # Si la línea contiene "País:" — acumular hasta formar un país válido
            m_pais = re.search(r'(?:País|Nacionalidad):\s*(.*?)$', linea, re.IGNORECASE)
            if m_pais:
                pais_parcial = m_pais.group(1).strip()

                # Seguir acumulando líneas si el país está incompleto
                while i + 1 < len(lineas_bloque):
                    next_line = lineas_bloque[i + 1].strip()

                    # Si la siguiente línea es un keyword, parar
                    if re.search(r'^(Insc\.|SOLICITADA|EN CLASE|PARA DISTINGUIR|TRAMITANTE|NOMBRE DE LA MARCA|DESCRIPCI)', next_line):
                        break

                    # Si ya tenemos un país válido y completo, parar
                    if pais_parcial.upper() in PAISES_CONOCIDOS:
                        break

                    # Si no tenemos país aún, la siguiente línea es probablemente el país
                    if not pais_parcial:
                        pais_parcial = next_line
                        linea = linea + ' ' + next_line
                        i += 1
                    # Si tenemos país parcial, probar si uniendo la siguiente línea forma un país válido
                    elif (pais_parcial + ' ' + next_line).upper() in PAISES_CONOCIDOS:
                        pais_parcial = pais_parcial + ' ' + next_line
                        linea = linea + ' ' + next_line
                        i += 1
                    else:
                        # La siguiente línea no es parte del país — es el nombre de marca
                        break

                lineas_unidas.append(linea)
                i += 1
            else:
                lineas_unidas.append(linea)
                i += 1

        texto_titular_raw = '\n'.join(lineas_unidas)

        # Buscar la última ocurrencia de "País: VALOR" o "Nacionalidad: VALOR" (con valor en la misma línea)
        partes_pais = list(re.finditer(r'(?:País|Nacionalidad):\s*\S+', texto_titular_raw, re.IGNORECASE))

        if partes_pais:
            ultimo_pais = partes_pais[-1]
            # Encontrar la línea que contiene el último País:
            lineas_titular = texto_titular_raw.split('\n')
            idx_pais = -1
            for idx, l in enumerate(lineas_titular):
                if re.search(r'(?:País|Nacionalidad):', l):
                    idx_pais = idx

            if idx_pais >= 0:
                # Titular = todo hasta e incluyendo la línea del país
                texto_para_titular = ' '.join(l.strip() for l in lineas_titular[:idx_pais + 1] if l.strip())
                titular = parsear_titular(texto_para_titular)
                entrada['titularNombre'] = titular['nombre']
                entrada['titularDomicilio'] = titular['domicilio']
                entrada['titularPais'] = titular['pais']

                # Nombre = lo que queda después de la línea del país
                lineas_nombre = [l.strip() for l in lineas_titular[idx_pais + 1:] if l.strip()]
                if lineas_nombre and not entrada['nombre']:
                    nombre_candidato = ' '.join(lineas_nombre)
                    if not any(kw in nombre_candidato.upper() for kw in ['EN CLASE', 'PARA DISTINGUIR', 'TRAMITANTE', 'DESCRIPCION']):
                        entrada['nombre'] = nombre_candidato
        else:
            titular = parsear_titular(texto_titular_raw)
            entrada['titularNombre'] = titular['nombre']
            entrada['titularDomicilio'] = titular['domicilio']
            entrada['titularPais'] = titular['pais']

    # 4. EN CLASE (dos formatos: "EN CLASE: 35" o "Clase 21" inline en PARA DISTINGUIR)
    m_clase = re.search(r'EN CLASE:\s*(\S+)', texto_completo)
    if m_clase:
        entrada['clase'] = m_clase.group(1).strip()
    else:
        # Formato viejo: "PARA DISTINGUIR: texto. Clase 21"
        m_clase_inline = re.search(r'[Cc]lase\s+(\d+|NC|LC)\s*$', texto_completo, re.MULTILINE)
        if m_clase_inline:
            entrada['clase'] = m_clase_inline.group(1).strip()

    # 5. PARA DISTINGUIR
    m_dist = re.search(r'PARA DISTINGUIR:\s*(.+?)(?=DESCRIPCI[ÓO]N DE ETIQUETA:|TRAMITANTE:|COMENTARIO:|REGISTROS NEGANTES:|$)', texto_completo, re.DOTALL)
    if m_dist:
        distingue = m_dist.group(1).strip()
        # Limpiar saltos de línea internos
        distingue = re.sub(r'\s*\n\s*', ' ', distingue).strip()
        # Formato viejo: "CRISTALERIA. Clase 21" — remover "Clase N" del final
        distingue = re.sub(r'\s*[Cc]lase\s+\d+\s*$', '', distingue).strip()
        entrada['distingue'] = distingue if distingue else None

    # 6. DESCRIPCION DE ETIQUETA (con y sin tilde en DESCRIPCIÓN)
    m_etiq = re.search(r'DESCRIPCI[ÓO]N DE ETIQUETA:\s*(.+?)(?=TRAMITANTE:|COMENTARIO:|REGISTROS NEGANTES:|$)', texto_completo, re.DOTALL)
    if m_etiq:
        etiqueta = m_etiq.group(1).strip()
        etiqueta = re.sub(r'\s*\n\s*', ' ', etiqueta).strip()
        entrada['descripcionEtiqueta'] = etiqueta if etiqueta else None

    # 7. TRAMITANTE
    m_tram = re.search(r'TRAMITANTE:\s*(.+?)(?=COMENTARIO:|REGISTROS NEGANTES:|$)', texto_completo, re.DOTALL)
    if m_tram:
        tramitante = m_tram.group(1).strip()
        tramitante = re.sub(r'\s*\n\s*', ' ', tramitante).strip()
        entrada['tramitante'] = tramitante if tramitante else None

    # 8. COMENTARIO (solo Negadas Art.27)
    m_com = re.search(r'COMENTARIO:\s*(.+?)(?=REGISTROS NEGANTES:|$)', texto_completo, re.DOTALL)
    if m_com:
        comentario = m_com.group(1).strip()
        comentario = re.sub(r'\s*\n\s*', ' ', comentario).strip()
        entrada['comentario'] = comentario if comentario else None

    # 9. REGISTROS NEGANTES (solo Negadas Art.33 / Observadas con lugar)
    m_neg = re.search(r'REGISTROS NEGANTES:\s*(.+?)$', texto_completo, re.DOTALL)
    if m_neg:
        neg_texto = m_neg.group(1).strip()
        neg_texto = re.sub(r'\s*\n\s*', ' ', neg_texto).strip()
        # Intentar parsear: REGISTRO Clase: N NOMBRE Titular: TITULAR
        m_neg_parsed = re.match(r'(\S+)\s+Clase:\s*(\S+)\s+(.+?)\s+Titular:\s*(.+)', neg_texto)
        if m_neg_parsed:
            entrada['registrosNegantes'] = [{
                'registro': m_neg_parsed.group(1),
                'clase': m_neg_parsed.group(2),
                'nombre': m_neg_parsed.group(3).strip(),
                'titular': m_neg_parsed.group(4).strip(),
            }]
        else:
            entrada['registrosNegantes'] = neg_texto

    return entrada


def parsear_tabla_resoluciones(texto: str, seccion: str) -> list[dict]:
    """Parsea formato B — tablas de resoluciones (Devueltas, Caducas, Oposiciones, Desistidas).

    Las entradas aparecen como bloques de texto sin separadores ___:
    nroSolicitud
    clase
    nombre marca
    titular Domicilio: dom País: pais
    tramitante
    """
    entradas = []
    lineas = texto.strip().split('\n')
    # NO filtrar con es_ruido() aquí — los números de clase (1-45)
    # son idénticos a números de página y se perderían.
    # Solo eliminar líneas vacías y headers conocidos.
    lineas = [l for l in lineas if l.strip() and
              not l.strip().startswith('Boletín de la Propiedad Industrial') and
              not re.match(r'^Tomo\s+[IVXLC]+\s*$', l.strip()) and
              not re.match(r'^No\.\s*\d+\s*$', l.strip()) and
              not l.strip().startswith('SOLICITUD CLASE') and
              not l.strip().startswith('NOMBRE DE LAS MARCAS') and
              not l.strip() in ('TITULAR', 'TRAMITANTE', 'SOLICITUD', 'CLASE')]

    # Buscar líneas que parecen nroSolicitud (YYYY-NNNNNN)
    i = 0
    while i < len(lineas):
        linea = lineas[i].strip()
        m_nro = re.match(r'^(\d{4}-\d{5,6})$', linea)
        if m_nro:
            nro = m_nro.group(1)
            # Acumular las líneas siguientes hasta el próximo nroSolicitud o header
            bloque_lineas = [linea]
            j = i + 1
            while j < len(lineas):
                next_line = lineas[j].strip()
                # Si la siguiente línea es otro nroSolicitud, parar
                if re.match(r'^\d{4}-\d{5,6}$', next_line):
                    break
                # Si es un header de sección, parar
                if any(kw in next_line.upper() for kw in ['RESOLUCIÓN', 'VISTAS LAS SOLICITUDES', 'SOLICITUD CLASE', 'NOMBRE DE LAS MARCAS']):
                    break
                bloque_lineas.append(next_line)
                j += 1

            # Parsear el bloque: clase, nombre, titular, tramitante
            entrada = {
                'seccion': seccion,
                'nroSolicitud': nro,
                'fechaInscripcion': None,
                'nombreMarca': None,
                'titularNombre': None,
                'titularDomicilio': None,
                'titularPais': None,
                'nombre': None,
                'clase': None,
                'distingue': None,
                'descripcionEtiqueta': None,
                'tramitante': None,
                'comentario': None,
                'registrosNegantes': None,
            }

            # Líneas restantes después del nroSolicitud
            lineas_dato = [l.strip() for l in bloque_lineas[1:] if l.strip()]

            # Clase: primera línea si es número 1-45 o NC/LC
            if lineas_dato and re.match(r'^(\d{1,2}|NC|LC)$', lineas_dato[0]):
                entrada['clase'] = lineas_dato[0]
                lineas_dato = lineas_dato[1:]

            # Unir resto para parsear
            resto = ' '.join(lineas_dato)

            # Buscar último "País: VALOR" para separar tramitante
            partes_pais = list(re.finditer(r'(?:País|Nacionalidad):\s*([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s.]+)', resto, re.IGNORECASE))
            if partes_pais:
                ultimo_pais = partes_pais[-1]
                pais_valor = ultimo_pais.group(1).strip()
                texto_antes_pais = resto[:ultimo_pais.start()]
                texto_tramitante = resto[ultimo_pais.end():].strip()

                if texto_tramitante:
                    entrada['tramitante'] = texto_tramitante

                entrada['titularPais'] = pais_valor

                # Separar nombre + titular de lo que está antes de País:
                # Buscar Domicilio: para obtener titular
                m_dom = re.search(r'Domicilio:', texto_antes_pais, re.IGNORECASE)
                if m_dom:
                    antes_dom = texto_antes_pais[:m_dom.start()].strip()
                    entrada['titularDomicilio'] = texto_antes_pais[m_dom.end():].strip().rstrip('.')

                    # antes_dom = nombre_marca + titular_nombre
                    # Separar usando las líneas originales como guía
                    # Las primeras líneas después de la clase son el nombre,
                    # luego viene el titular con Domicilio:
                    lineas_pre_dom = []
                    for l in lineas_dato:
                        if 'Domicilio:' in l:
                            # Esta línea contiene el inicio del titular
                            parte_pre = l[:l.index('Domicilio:')].strip()
                            if parte_pre:
                                lineas_pre_dom.append(parte_pre)
                            break
                        lineas_pre_dom.append(l)

                    if len(lineas_pre_dom) >= 2:
                        # Primera(s) línea(s) = nombre, última = titular
                        # Heurística: buscar patrón empresa en la última línea
                        entrada['nombre'] = lineas_pre_dom[0]
                        entrada['titularNombre'] = ' '.join(lineas_pre_dom[1:])
                    elif len(lineas_pre_dom) == 1:
                        # Solo una línea — puede ser nombre+titular juntos o solo titular
                        entrada['titularNombre'] = lineas_pre_dom[0]
                else:
                    # Sin Domicilio: — todo antes de País: es nombre+titular
                    entrada['titularNombre'] = texto_antes_pais.strip()
            elif resto:
                entrada['nombre'] = resto

            entradas.append(entrada)
            i = j
        else:
            i += 1

    return entradas


def parsear_tabla_registros(texto: str, seccion: str) -> list[dict]:
    """Parsea formatos C-H — tablas de registro (Renovaciones, Cesiones, etc.).

    Las entradas usan nroRegistro (D014582, F058467, etc.) en vez de nroSolicitud.
    El formato de columnas varía por sección:
      C (Renovaciones):    REGISTRO TIPO MARCA CLASE TITULAR VIGENTE TRAMITANTE
      D (Cambio nombre):   REGISTRO TIPO MARCA TITULAR_ANTERIOR TITULAR_ACTUAL TRAMITANTE
      E (Cesiones):        REGISTRO TIPO MARCA CEDENTE CESIONARIO TRAMITANTE
      F (Fusiones):        REGISTRO TIPO MARCA EMPRESA_FUSIONARSE EMPRESA_SOBREVIVIENTE DOMICILIO_SOBREVIVIENTE TRAMITANTE
      G (Cambio domicilio):REGISTRO TIPO MARCA TITULAR DOMICILIO_ANTERIOR DOMICILIO_ACTUAL TRAMITANTE
      H (Licencias):       REGISTRO TIPO MARCA LICENCIANTE LICENCIATARIO DOMICILIO_LICENCIATARIO TRAMITANTE
    """
    entradas = []
    lineas = texto.strip().split('\n')
    # Filtrar solo headers de tabla y líneas vacías, NO números
    lineas = [l for l in lineas if l.strip() and
              not l.strip().startswith('Boletín de la Propiedad Industrial') and
              not re.match(r'^Tomo\s+[IVXLC]+\s*$', l.strip()) and
              not re.match(r'^No\.\s*\d+\s*$', l.strip()) and
              not l.strip() in ('REGISTRO', 'TIPO', 'MARCA', 'CLASE INT', 'TITULAR',
                                'VIGENTE', 'TRAMITANTE', 'CEDENTE', 'CESIONARIO',
                                'TITULAR ANTERIOR', 'TITULAR ACTUAL',
                                'EMPRESA A', 'FUSIONARSE', 'EMPRESA', 'SOBREVIVIENTE',
                                'DOMICILIO DEL', 'SOBREVIVIENTE', 'LICENCIANTE',
                                'LICENCIATARIO', 'DOMICILIO ANTERIOR', 'DOMICILIO ACTUAL')]

    # Patrón de nroRegistro: letra + 5-6 dígitos
    NRO_REG = re.compile(r'^([A-Z]\d{5,6})\s*$')

    i = 0
    while i < len(lineas):
        linea = lineas[i].strip()
        m_reg = NRO_REG.match(linea)
        if m_reg:
            nro_registro = m_reg.group(1)

            # Acumular líneas hasta el próximo registro
            bloque = [linea]
            j = i + 1
            while j < len(lineas):
                next_line = lineas[j].strip()
                if NRO_REG.match(next_line):
                    break
                # Parar en headers de sección
                if any(kw in next_line.upper() for kw in ['RESOLUCIÓN', 'VISTA LAS SOLICITUDES',
                       'NOTIFICA LOS SIGUIENTES', 'NOTIFICA LAS SIGUIENTES',
                       'SE DEJA CONSTANCIA', 'Total de Solicitudes']):
                    break
                bloque.append(next_line)
                j += 1

            # Parsear según la sección
            lineas_dato = bloque[1:]  # sin el nroRegistro
            entrada = {
                'seccion': seccion,
                'nroSolicitud': nro_registro,  # guardamos nroRegistro en nroSolicitud por ahora
                'fechaInscripcion': None,
                'nombreMarca': None,
                'titularNombre': None,
                'titularDomicilio': None,
                'titularPais': None,
                'nombre': None,
                'clase': None,
                'distingue': None,
                'descripcionEtiqueta': None,
                'tramitante': None,
                'comentario': None,
                'registrosNegantes': None,
                # Campos de registro
                'tipoRegistro': None,
                'fechaVigente': None,
                'cedente': None,
                'cesionario': None,
                'titularAnterior': None,
                'titularActual': None,
                'empresaFusionarse': None,
                'empresaSobreviviente': None,
                'domicilioSobreviviente': None,
                'domicilioAnterior': None,
                'domicilioActual': None,
                'licenciante': None,
                'licenciatario': None,
                'domicilioLicenciatario': None,
            }

            # TIPO: siguiente línea (MP, MS, NC, LC)
            if lineas_dato and re.match(r'^(MP|MS|NC|LC)$', lineas_dato[0]):
                entrada['tipoRegistro'] = lineas_dato[0]
                lineas_dato = lineas_dato[1:]

            # MARCA: siguiente línea(s) hasta la clase o dato específico
            # Dependiendo de la sección, los campos cambian

            if seccion == 'RENOVACIONES':
                # MARCA, CLASE, TITULAR, VIGENTE (DD/MM/YYYY), TRAMITANTE
                # Clase puede ser número o NC/LC
                nombre_lines = []
                while lineas_dato and not re.match(r'^(\d{1,2}|NC|LC)$', lineas_dato[0]):
                    nombre_lines.append(lineas_dato.pop(0))
                entrada['nombre'] = ' '.join(nombre_lines) if nombre_lines else None

                if lineas_dato and re.match(r'^(\d{1,2}|NC|LC)$', lineas_dato[0]):
                    entrada['clase'] = lineas_dato.pop(0)

                # Buscar fecha vigente (DD/MM/YYYY)
                resto = ' '.join(lineas_dato)
                m_fecha = re.search(r'(\d{2}/\d{2}/\d{4})', resto)
                if m_fecha:
                    entrada['fechaVigente'] = m_fecha.group(1)
                    antes_fecha = resto[:m_fecha.start()].strip()
                    despues_fecha = resto[m_fecha.end():].strip()
                    entrada['titularNombre'] = antes_fecha if antes_fecha else None
                    entrada['tramitante'] = despues_fecha if despues_fecha else None

            elif seccion == 'CAMBIOS_NOMBRE':
                # MARCA, TITULAR ANTERIOR, TITULAR ACTUAL, TRAMITANTE
                resto = ' '.join(l.strip() for l in lineas_dato)
                entrada['nombre'] = None  # se extrae abajo
                # Difícil separar sin keywords, dejar como texto crudo
                entrada['titularNombre'] = resto

            elif seccion == 'CESIONES':
                # MARCA, CEDENTE, CESIONARIO, TRAMITANTE
                resto = ' '.join(l.strip() for l in lineas_dato)
                entrada['nombre'] = None
                entrada['titularNombre'] = resto

            elif seccion == 'FUSIONES':
                # MARCA, EMPRESA FUSIONARSE, EMPRESA SOBREVIVIENTE, DOMICILIO, TRAMITANTE
                resto = ' '.join(l.strip() for l in lineas_dato)
                entrada['nombre'] = None
                entrada['titularNombre'] = resto

            elif seccion == 'CAMBIOS_DOMICILIO':
                # MARCA, TITULAR, DOMICILIO ANTERIOR, DOMICILIO ACTUAL, TRAMITANTE
                resto = ' '.join(l.strip() for l in lineas_dato)
                entrada['nombre'] = None
                entrada['titularNombre'] = resto

            elif seccion == 'LICENCIAS':
                # MARCA, LICENCIANTE, LICENCIATARIO, DOMICILIO, TRAMITANTE
                resto = ' '.join(l.strip() for l in lineas_dato)
                entrada['nombre'] = None
                entrada['titularNombre'] = resto

            else:
                # Genérico
                resto = ' '.join(l.strip() for l in lineas_dato)
                entrada['nombre'] = resto

            entradas.append(entrada)
            i = j
        else:
            i += 1

    return entradas


def detectar_seccion(texto: str) -> str:
    """Detecta la sección del boletín por el header."""
    secciones = [
        ('MARCAS CON ORDEN DE PUBLICACIÓN EN PRENSA', 'MARCAS_PUBLICACION_PRENSA'),
        ('MARCAS SOLICITADAS A EFECTO DE OPOSICI', 'MARCAS_SOLICITADAS_OPOSICION'),
        ('SOLICITUDES DE MARCAS DE PRODUCTOS CONCEDID', 'MARCAS_PRODUCTOS_CONCEDIDAS'),
        ('SOLICITUDES DE MARCAS DE SERVICIOS', 'MARCAS_SERVICIOS_CONCEDIDAS'),
        ('SOLICITUDES DE NOMBRES COMERCIALES CONCEDID', 'NOMBRES_COMERCIALES_CONCEDIDOS'),
        ('SOLICITUDES DE LEMAS COMERCIALES CONCEDID', 'LEMAS_COMERCIALES_CONCEDIDOS'),
        ('NEGADAS', 'NEGADAS'),
        ('OBSERVADAS RESUELTAS', 'OBSERVADAS_RESUELTAS'),
        ('DEVUELTAS DE FORMA', 'DEVUELTAS_FORMA'),
        ('DEVUELTAS DE FONDO', 'DEVUELTAS_FONDO'),
        ('OPOSICI', 'OPOSICIONES'),
        ('DESISTID', 'DESISTIDAS'),
        ('CADUCAS', 'CADUCAS'),
        ('RENOVACIONES', 'RENOVACIONES'),
        ('CAMBIO DE NOMBRE', 'CAMBIOS_NOMBRE'),
        ('CESIONES', 'CESIONES'),
        ('FUSIONES', 'FUSIONES'),
        ('CAMBIO DE DOMICILIO', 'CAMBIOS_DOMICILIO'),
        ('LICENCIAS DE USO', 'LICENCIAS'),
        ('PATENTES DE INVENCIÓN', 'PATENTES_INVENCION'),
        ('MODELO INDUSTRIAL', 'PATENTES_MODELO'),
        ('DIBUJO INDUSTRIAL', 'PATENTES_DIBUJO'),
        ('DISPOSICIONES ADMINISTRATIV', 'DISPOSICIONES'),
        ('RECURSOS JERÁRQUIC', 'RECURSOS_JERARQUICOS'),
    ]
    for pattern, nombre in secciones:
        if pattern.upper() in texto.upper():
            return nombre
    return 'DESCONOCIDA'


def limpiar_paginacion(texto: str) -> str:
    """Elimina ruido de paginación del texto completo ANTES de dividir en bloques.

    Esto resuelve el problema de entradas que cruzan saltos de página.
    Patrones a eliminar:
      - Líneas con solo un número (número de página): "17", "33", etc.
      - "Tomo I", "Tomo IX", etc.
      - "Boletín de la Propiedad Industrial" + "No. 651"
      - Líneas vacías redundantes
    """
    lineas = texto.split('\n')
    resultado = []
    i = 0
    while i < len(lineas):
        linea = lineas[i]
        s = linea.strip()

        # Saltar número de página solo (1-3 dígitos)
        # PERO NO si la línea anterior es un nroSolicitud (entonces es clase Niza)
        # Insertar marcador de página para rastreo de imágenes
        if re.match(r'^\d{1,3}$', s):
            prev = resultado[-1].strip() if resultado else ''
            if not re.match(r'^\d{4}-\d{5,6}$', prev):
                i += 1
                continue

        # Saltar "Tomo I", "Tomo IX", "Tomo XVII", etc.
        if re.match(r'^Tomo\s+[IVXLC]+\s*$', s):
            i += 1
            continue

        # Saltar header del boletín
        if s.startswith('Boletín de la Propiedad Industrial') or re.match(r'^No\.\s*\d+\s*$', s):
            i += 1
            continue

        # Saltar líneas completamente vacías (dejar máximo 1)
        if not s:
            if resultado and not resultado[-1].strip():
                i += 1
                continue

        resultado.append(linea)
        i += 1

    return '\n'.join(resultado)


def parsear_patente_wipo(texto: str, seccion: str) -> dict | None:
    """Parsea formato I — patentes WIPO con códigos numéricos.

    Códigos: (11) nroPublicacion, (21) nroSolicitud, (22) fechaPresentacion,
    (30) prioridad, (45) fechaConcesion, (51) clasificacionIPC,
    (54) titulo, (57) resumen, (72) inventores, (73) titular, (74) tramitante
    """
    # Extraer campos por código WIPO
    campos = {}
    # Buscar patrones (NN) seguidos de su valor (en la misma línea o siguiente)
    for m in re.finditer(r'\((\d{2})\)\s*(.*?)(?=\(\d{2}\)|$)', texto, re.DOTALL):
        codigo = m.group(1)
        valor = m.group(2).strip()
        # Limpiar saltos de línea
        valor = re.sub(r'\s*\n\s*', ' ', valor).strip()
        campos[codigo] = valor

    nro_solicitud = campos.get('21', '').strip()
    if not nro_solicitud:
        return None

    entrada = {
        'seccion': seccion,
        'nroSolicitud': nro_solicitud,
        'fechaInscripcion': None,
        'nombreMarca': None,
        'titularNombre': None,
        'titularDomicilio': None,
        'titularPais': None,
        'nombre': campos.get('54', '').strip() or None,  # título de la patente
        'clase': campos.get('51', '').strip() or None,  # clasificación IPC
        'distingue': campos.get('57', '').strip() or None,  # resumen
        'descripcionEtiqueta': None,
        'tramitante': campos.get('74', '').strip() or None,
        'comentario': None,
        'registrosNegantes': None,
        # Campos de patente
        'numeroPublicacion': campos.get('11', '').strip() or None,
        'prioridadPatente': campos.get('30', '').strip() or None,
        'fechaConcesionPatente': campos.get('45', '').strip() or None,
        'inventores': campos.get('72', '').strip() or None,
    }

    # Parsear titular (73)
    titular_raw = campos.get('73', '')
    if titular_raw:
        titular = parsear_titular(titular_raw)
        entrada['titularNombre'] = titular['nombre']
        entrada['titularDomicilio'] = titular['domicilio']
        entrada['titularPais'] = titular['pais']

    # Fecha presentación (22)
    fecha_raw = campos.get('22', '')
    if fecha_raw:
        # Formato DD/MM/YYYY
        m_fecha = re.match(r'(\d{2})/(\d{2})/(\d{4})', fecha_raw)
        if m_fecha:
            entrada['fechaInscripcion'] = f"{m_fecha.group(3)}-{m_fecha.group(2)}-{m_fecha.group(1)}"

    return entrada


SECCIONES_HEADERS = [
    ('MARCAS CON ORDEN DE PUBLICACIÓN EN PRENSA', 'MARCAS_PUBLICACION_PRENSA'),
    ('ORDEN DE PUBLICACIÓN EN PRENSA', 'MARCAS_PUBLICACION_PRENSA'),
    ('MARCAS COMERCIALES SOLICITADAS', 'MARCAS_SOLICITADAS_OPOSICION'),
    ('MARCAS SOLICITADAS A EFECTO DE OPOSICI', 'MARCAS_SOLICITADAS_OPOSICION'),
    ('SOLICITUDES DE MARCAS DE PRODUCTOS CONCEDID', 'MARCAS_PRODUCTOS_CONCEDIDAS'),
    ('MARCAS DE PRODUCTOS CONCEDIDAS', 'MARCAS_PRODUCTOS_CONCEDIDAS'),
    ('SOLICITUDES DE MARCAS DE SERVICIOS', 'MARCAS_SERVICIOS_CONCEDIDAS'),
    ('MARCAS DE SERVICIOS CONCEDIDAS', 'MARCAS_SERVICIOS_CONCEDIDAS'),
    ('SOLICITUDES DE NOMBRES COMERCIALES CONCEDID', 'NOMBRES_COMERCIALES_CONCEDIDOS'),
    ('MARCAS DE NOMBRES CONCEDID', 'NOMBRES_COMERCIALES_CONCEDIDOS'),
    ('SOLICITUDES DE LEMAS COMERCIALES CONCEDID', 'LEMAS_COMERCIALES_CONCEDIDOS'),
    ('MARCAS DE LEMAS CONCEDID', 'LEMAS_COMERCIALES_CONCEDIDOS'),
    ('NEGADAS', 'NEGADAS'),
    ('OPOSICIONES SIN LUGAR, SE OTORGA', 'OBSERVADAS_SIN_LUGAR'),
    ('OPOSICIONES CON LUGAR, SE NIEGA', 'OBSERVADAS_CON_LUGAR'),
    ('DEVUELTAS DE FORMA', 'DEVUELTAS_FORMA'),
    ('DEVUELTAS DE FONDO', 'DEVUELTAS_FONDO'),
    ('NOTIFICACI.*OPOSICI', 'OPOSICIONES'),
    ('DESISTID', 'DESISTIDAS'),
    ('CADUCAS POR NO PAGO', 'CADUCAS'),
    # No buscar PRIORIDAD EXTINGUIDA como sección — aparece en el preámbulo legal de Devueltas
    # ('PRIORIDAD EXTINGUIDA', 'PRIORIDAD_EXTINGUIDA'),
    ('RENOVACIONES DE MARCAS', 'RENOVACIONES'),
    ('CAMBIO DE NOMBRE DE MARCAS', 'CAMBIOS_NOMBRE'),
    ('CESIONES DE MARCAS', 'CESIONES'),
    ('FUSIONES DE MARCAS', 'FUSIONES'),
    ('CAMBIO DE DOMICILIO DE MARCAS', 'CAMBIOS_DOMICILIO'),
    ('LICENCIAS DE USO', 'LICENCIAS'),
    ('PATENTE DE INVENCIÓN PUBLICAD', 'PATENTES_INVENCION'),
    ('MODELO INDUSTRIAL PUBLICAD', 'PATENTES_MODELO'),
    ('DIBUJO INDUSTRIAL PUBLICAD', 'PATENTES_DIBUJO'),
    ('PATENTES DE INVENCIÓN CONCEDIDAS', 'PATENTES_CONCEDIDAS'),
    ('PATENTES DE INVENCIÓN DESISTIDAS', 'PATENTES_DESISTIDAS'),
    ('PATENTES.*DEVUELTAS', 'PATENTES_DEVUELTAS'),
    ('ORDEN DE PUBLICACIÓN EN PRENSA DE SOLICITUDES DE PATENTE', 'PATENTES_PUBLICACION_PRENSA'),
    ('DISPOSICIONES ADMINISTRATIV', 'DISPOSICIONES'),
    ('RECURSOS JERÁRQUIC', 'RECURSOS_JERARQUICOS'),
]

SECCIONES_TABLA_RESOLUCIONES = {'DEVUELTAS_FORMA', 'DEVUELTAS_FONDO', 'OPOSICIONES',
                                 'DESISTIDAS', 'CADUCAS', 'PRIORIDAD_EXTINGUIDA'}
SECCIONES_TABLA_REGISTROS = {'RENOVACIONES', 'CAMBIOS_NOMBRE', 'CESIONES',
                              'FUSIONES', 'CAMBIOS_DOMICILIO', 'LICENCIAS'}
SECCIONES_FORMATO_A = {'MARCAS_PUBLICACION_PRENSA', 'MARCAS_SOLICITADAS_OPOSICION',
                        'MARCAS_PRODUCTOS_CONCEDIDAS', 'MARCAS_SERVICIOS_CONCEDIDAS',
                        'NOMBRES_COMERCIALES_CONCEDIDOS', 'LEMAS_COMERCIALES_CONCEDIDOS',
                        'NEGADAS', 'OBSERVADAS_SIN_LUGAR', 'OBSERVADAS_CON_LUGAR'}


def procesar_tomo(ruta_texto: str) -> list[dict]:
    """Procesa un archivo de texto completo de un tomo.

    Approach: primero escanear todo el texto para encontrar secciones,
    después procesar cada sección con el parser correcto.
    """
    with open(ruta_texto, 'r', encoding='utf-8') as f:
        contenido = f.read()

    # PASO 1: limpiar paginación
    contenido = limpiar_paginacion(contenido)

    # PASO 2: encontrar todas las secciones y sus posiciones
    secciones_encontradas = []
    for pattern, nombre in SECCIONES_HEADERS:
        for m in re.finditer(pattern, contenido, re.IGNORECASE):
            secciones_encontradas.append((m.start(), nombre))

    # Ordenar por posición
    secciones_encontradas.sort(key=lambda x: x[0])

    # PASO 3: dividir el texto en segmentos por sección
    entradas = []

    if not secciones_encontradas:
        # Sin secciones detectadas — intentar procesar como bloque único
        # (puede ser un tomo con una sola sección)
        bloques = re.split(r'_{5,}', contenido)
        for bloque in bloques:
            if re.search(r'Insc\.\s*\d', bloque):
                lineas = limpiar_bloque(bloque.strip().split('\n'))
                entrada = parsear_bloque_inscripcion(lineas)
                if entrada:
                    entrada['seccion'] = 'DESCONOCIDA'
                    entradas.append(entrada)
        return entradas

    # Procesar cada segmento de sección
    for idx, (pos, seccion) in enumerate(secciones_encontradas):
        # El segmento va desde esta sección hasta la siguiente
        fin = secciones_encontradas[idx + 1][0] if idx + 1 < len(secciones_encontradas) else len(contenido)
        segmento = contenido[pos:fin]

        if seccion in SECCIONES_FORMATO_A:
            # Dividir por ___ y parsear cada bloque como inscripción
            bloques = re.split(r'_{5,}', segmento)
            for bloque in bloques:
                if re.search(r'Insc\.\s*\d', bloque):
                    lineas = limpiar_bloque(bloque.strip().split('\n'))
                    entrada = parsear_bloque_inscripcion(lineas)
                    if entrada:
                        entrada['seccion'] = seccion
                        entradas.append(entrada)

        elif seccion in SECCIONES_TABLA_RESOLUCIONES:
            if re.search(r'^\d{4}-\d{5,6}$', segmento, re.MULTILINE):
                entradas_tabla = parsear_tabla_resoluciones(segmento, seccion)
                entradas.extend(entradas_tabla)

        elif seccion in SECCIONES_TABLA_REGISTROS:
            if re.search(r'^[A-Z]\d{5,6}\s*$', segmento, re.MULTILINE):
                entradas_reg = parsear_tabla_registros(segmento, seccion)
                entradas.extend(entradas_reg)

        elif seccion in ('PATENTES_INVENCION', 'PATENTES_MODELO', 'PATENTES_DIBUJO',
                         'PATENTES_CONCEDIDAS', 'PATENTES_DESISTIDAS'):
            # Formato WIPO: cada entrada entre ___ tiene códigos (11), (21), etc.
            bloques = re.split(r'_{5,}', segmento)
            for bloque in bloques:
                if '(11)' in bloque or '(21)' in bloque:
                    entrada = parsear_patente_wipo(bloque, seccion)
                    if entrada:
                        entradas.append(entrada)

        # TODO: agregar parser para disposiciones administrativas (formato K)

    return entradas


def main():
    if len(sys.argv) < 2:
        print("Uso: python3 extractor/extraer_ia.py <ruta_texto_completo>")
        print("  Ej: python3 extractor/extraer_ia.py extracciones/B651/texto/b651_tomo1_completo.txt")
        sys.exit(1)

    ruta = sys.argv[1]
    print(f"Procesando: {ruta}")

    entradas = procesar_tomo(ruta)

    # Guardar JSON
    tomo_name = Path(ruta).stem.replace('_completo', '')
    ruta_salida = Path(ruta).parent.parent / f'entradas_ia_{tomo_name}.json'
    with open(ruta_salida, 'w', encoding='utf-8') as f:
        json.dump(entradas, f, indent=2, ensure_ascii=False)

    print(f"Entradas extraídas: {len(entradas)}")
    print(f"Guardado en: {ruta_salida}")

    # Resumen por sección
    secciones = {}
    for e in entradas:
        s = e.get('seccion', 'DESCONOCIDA')
        secciones[s] = secciones.get(s, 0) + 1

    print("\nPor sección:")
    for s, c in sorted(secciones.items()):
        print(f"  {s}: {c}")

    # Mostrar primeras 3 entradas como ejemplo
    print("\nPrimeras 3 entradas:")
    for e in entradas[:3]:
        print(f"  {e['nroSolicitud']} | {e['nombre'] or '(figurativa)'} | Clase {e['clase']} | {e['seccion']}")


if __name__ == '__main__':
    main()
