#!/usr/bin/env python3
"""
Fase 1: Extracción bruta de boletines SAPI.

Lee PDFs con pymupdf y genera:
  - metadata.json (info del boletín y tomos)
  - texto/{tomo}_completo.txt (texto raw completo por tomo)
  - texto/{tomo}_p{NNN}.txt (texto raw por página)
  - imagenes/{tomo}_p{NNN}_img{N}.{ext} (imágenes extraídas)

Uso:
  python3 extractor/extraer.py /ruta/a/boletines /ruta/salida
  python3 extractor/extraer.py /ruta/a/boletines /ruta/salida --solo 651
  python3 extractor/extraer.py /ruta/a/boletines /ruta/salida --solo 651,520,464
"""

import fitz  # pymupdf
import os
import sys
import json
import re
import time
import hashlib
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional


# ============================================
# CONFIGURACIÓN
# ============================================

# Imágenes menores a este tamaño se descartan (decorativas, líneas, etc.)
MIN_IMAGE_WIDTH = 50
MIN_IMAGE_HEIGHT = 50
MIN_IMAGE_BYTES = 500


# ============================================
# TIPOS
# ============================================

@dataclass
class ImagenExtraida:
    archivo: str
    pagina: int
    indice: int
    ancho: int
    alto: int
    formato: str
    bytes: int
    x0: float
    y0: float
    x1: float
    y1: float


@dataclass
class TomoExtraido:
    archivo_pdf: str
    ruta_pdf: str
    paginas: int
    chars_total: int
    imagenes: int
    imagenes_filtradas: int  # después de filtrar decorativas
    archivo_texto: str  # ruta relativa al txt completo
    archivos_paginas: list  # rutas relativas a txts por página
    archivos_imagenes: list  # rutas relativas a imágenes
    imagenes_meta: list  # metadata de cada imagen
    checksum_texto: str  # sha256 del texto completo
    tiempo_ms: int
    errores: list


@dataclass
class BoletinExtraido:
    numero: str
    carpeta_origen: str
    era: str
    tomos: list
    total_paginas: int
    total_chars: int
    total_imagenes: int
    total_imagenes_filtradas: int
    tiempo_ms: int
    errores: list


# ============================================
# UTILIDADES
# ============================================

def extraer_numero_boletin(carpeta: str) -> str:
    """Extrae número de boletín del nombre de carpeta."""
    nombre = os.path.basename(carpeta)
    if "Extraordinario" in nombre:
        m = re.search(r'No\.\s*(\d+)', nombre)
        return f"E{m.group(1)}" if m else nombre
    m = re.search(r'(\d{3,4})', nombre)
    return m.group(1) if m else nombre


def clasificar_era(numero: str) -> str:
    """Clasifica boletín por era."""
    if numero.startswith("E"):
        return "extraordinario"
    try:
        n = int(numero)
        if n < 483:
            return "vieja"
        elif n < 600:
            return "media"
        else:
            return "nueva"
    except ValueError:
        return "desconocida"


def encontrar_pdfs(carpeta: str) -> list:
    """Encuentra todos los PDFs recursivamente, ordenados."""
    pdfs = []
    for root, _, files in os.walk(carpeta):
        for f in files:
            if f.lower().endswith('.pdf'):
                pdfs.append(os.path.join(root, f))
    return sorted(pdfs)


def nombre_tomo(ruta_pdf: str, carpeta_boletin: str) -> str:
    """Genera un nombre limpio para el tomo basado en el nombre del PDF."""
    rel = os.path.relpath(ruta_pdf, carpeta_boletin)
    # Limpiar: quitar extensión, reemplazar separadores
    nombre = Path(rel).stem
    # Normalizar: quitar caracteres problemáticos
    nombre = re.sub(r'[^\w\-.]', '_', nombre)
    nombre = re.sub(r'_+', '_', nombre).strip('_')
    return nombre.lower()


def sha256_texto(texto: str) -> str:
    """Calcula SHA256 del texto."""
    return hashlib.sha256(texto.encode('utf-8')).hexdigest()


# ============================================
# EXTRACCIÓN
# ============================================

def extraer_tomo(ruta_pdf: str, dir_salida: str, carpeta_boletin: str) -> TomoExtraido:
    """Extrae texto e imágenes de un PDF individual."""
    inicio = time.time()
    tomo_nombre = nombre_tomo(ruta_pdf, carpeta_boletin)

    resultado = TomoExtraido(
        archivo_pdf=os.path.basename(ruta_pdf),
        ruta_pdf=ruta_pdf,
        paginas=0,
        chars_total=0,
        imagenes=0,
        imagenes_filtradas=0,
        archivo_texto="",
        archivos_paginas=[],
        archivos_imagenes=[],
        imagenes_meta=[],
        checksum_texto="",
        tiempo_ms=0,
        errores=[],
    )

    dir_texto = os.path.join(dir_salida, "texto")
    dir_imagenes = os.path.join(dir_salida, "imagenes")
    os.makedirs(dir_texto, exist_ok=True)
    os.makedirs(dir_imagenes, exist_ok=True)

    try:
        doc = fitz.open(ruta_pdf)
    except Exception as e:
        resultado.errores.append(f"No se pudo abrir PDF: {e}")
        resultado.tiempo_ms = int((time.time() - inicio) * 1000)
        return resultado

    resultado.paginas = len(doc)
    texto_completo = []

    for page_num in range(len(doc)):
        page = doc[page_num]

        # --- Texto ---
        texto_pagina = page.get_text()
        texto_completo.append(texto_pagina)

        # Guardar texto por página
        archivo_pagina = f"{tomo_nombre}_p{page_num + 1:03d}.txt"
        ruta_archivo_pagina = os.path.join(dir_texto, archivo_pagina)
        with open(ruta_archivo_pagina, 'w', encoding='utf-8') as f:
            f.write(texto_pagina)
        resultado.archivos_paginas.append(f"texto/{archivo_pagina}")

        # --- Imágenes ---
        imagenes = page.get_images(full=True)
        resultado.imagenes += len(imagenes)

        for img_idx, img_info in enumerate(imagenes):
            xref = img_info[0]
            try:
                base_img = doc.extract_image(xref)
                if not base_img:
                    continue

                ext = base_img.get('ext', 'png')
                img_bytes = base_img['image']
                w = base_img.get('width', 0)
                h = base_img.get('height', 0)

                # Filtrar imágenes decorativas
                if w < MIN_IMAGE_WIDTH or h < MIN_IMAGE_HEIGHT or len(img_bytes) < MIN_IMAGE_BYTES:
                    continue

                resultado.imagenes_filtradas += 1

                # Obtener posición de la imagen en la página
                img_rects = page.get_image_rects(xref)
                x0, y0, x1, y1 = 0, 0, 0, 0
                if img_rects:
                    rect = img_rects[0]
                    x0, y0, x1, y1 = rect.x0, rect.y0, rect.x1, rect.y1

                # Guardar imagen
                archivo_img = f"{tomo_nombre}_p{page_num + 1:03d}_img{img_idx}.{ext}"
                ruta_img = os.path.join(dir_imagenes, archivo_img)
                with open(ruta_img, 'wb') as f:
                    f.write(img_bytes)

                resultado.archivos_imagenes.append(f"imagenes/{archivo_img}")
                resultado.imagenes_meta.append(asdict(ImagenExtraida(
                    archivo=f"imagenes/{archivo_img}",
                    pagina=page_num + 1,
                    indice=img_idx,
                    ancho=w,
                    alto=h,
                    formato=ext,
                    bytes=len(img_bytes),
                    x0=round(x0, 1),
                    y0=round(y0, 1),
                    x1=round(x1, 1),
                    y1=round(y1, 1),
                )))

            except Exception as e:
                resultado.errores.append(
                    f"Error imagen p{page_num + 1} img{img_idx}: {e}"
                )

    doc.close()

    # Guardar texto completo
    texto_final = "\n".join(texto_completo)
    resultado.chars_total = len(texto_final)
    resultado.checksum_texto = sha256_texto(texto_final)

    archivo_completo = f"{tomo_nombre}_completo.txt"
    with open(os.path.join(dir_texto, archivo_completo), 'w', encoding='utf-8') as f:
        f.write(texto_final)
    resultado.archivo_texto = f"texto/{archivo_completo}"

    resultado.tiempo_ms = int((time.time() - inicio) * 1000)
    return resultado


def extraer_boletin(carpeta: str, dir_salida_base: str) -> BoletinExtraido:
    """Extrae un boletín completo (todos sus tomos)."""
    inicio = time.time()
    numero = extraer_numero_boletin(carpeta)
    era = clasificar_era(numero)

    resultado = BoletinExtraido(
        numero=numero,
        carpeta_origen=carpeta,
        era=era,
        tomos=[],
        total_paginas=0,
        total_chars=0,
        total_imagenes=0,
        total_imagenes_filtradas=0,
        tiempo_ms=0,
        errores=[],
    )

    # Directorio de salida para este boletín
    dir_boletin = os.path.join(dir_salida_base, f"B{numero}")
    os.makedirs(dir_boletin, exist_ok=True)

    pdfs = encontrar_pdfs(carpeta)
    if not pdfs:
        resultado.errores.append("Sin PDFs encontrados")
        resultado.tiempo_ms = int((time.time() - inicio) * 1000)
        return resultado

    for pdf in pdfs:
        tomo = extraer_tomo(pdf, dir_boletin, carpeta)
        resultado.tomos.append(asdict(tomo))
        resultado.total_paginas += tomo.paginas
        resultado.total_chars += tomo.chars_total
        resultado.total_imagenes += tomo.imagenes
        resultado.total_imagenes_filtradas += tomo.imagenes_filtradas
        resultado.errores.extend(tomo.errores)

    resultado.tiempo_ms = int((time.time() - inicio) * 1000)

    # Guardar metadata
    with open(os.path.join(dir_boletin, "metadata.json"), 'w', encoding='utf-8') as f:
        json.dump(asdict(resultado), f, indent=2, ensure_ascii=False)

    return resultado


# ============================================
# MAIN
# ============================================

def main():
    if len(sys.argv) < 3:
        print("Uso: python3 extractor/extraer.py <carpeta_boletines> <carpeta_salida> [--solo N,N,N]")
        sys.exit(1)

    dir_boletines = sys.argv[1]
    dir_salida = sys.argv[2]

    # Filtro opcional
    filtro = set()
    if '--solo' in sys.argv:
        idx = sys.argv.index('--solo')
        if idx + 1 < len(sys.argv):
            filtro = set(sys.argv[idx + 1].split(','))

    # Encontrar carpetas de boletines
    carpetas = []
    for nombre in sorted(os.listdir(dir_boletines)):
        ruta = os.path.join(dir_boletines, nombre)
        if not os.path.isdir(ruta):
            continue
        numero = extraer_numero_boletin(ruta)
        if filtro and numero not in filtro:
            continue
        carpetas.append(ruta)

    print(f"Fase 1: Extracción bruta de {len(carpetas)} boletines")
    print(f"Origen: {dir_boletines}")
    print(f"Salida: {dir_salida}")
    print("=" * 80)

    os.makedirs(dir_salida, exist_ok=True)
    resultados = []
    total_inicio = time.time()

    for i, carpeta in enumerate(carpetas):
        numero = extraer_numero_boletin(carpeta)
        print(f"[{i + 1}/{len(carpetas)}] B{numero}...", end=" ", flush=True)

        r = extraer_boletin(carpeta, dir_salida)
        resultados.append(asdict(r))

        status = "✓" if not r.errores else f"⚠ ({len(r.errores)} errores)"
        print(f"{status} {len(r.tomos)} tomos, {r.total_paginas} págs, "
              f"{r.total_chars:,} chars, {r.total_imagenes_filtradas} imgs "
              f"({r.tiempo_ms}ms) [{r.era}]")

    total_ms = int((time.time() - total_inicio) * 1000)

    # Resumen
    total_paginas = sum(r['total_paginas'] for r in resultados)
    total_chars = sum(r['total_chars'] for r in resultados)
    total_imgs = sum(r['total_imagenes_filtradas'] for r in resultados)
    total_imgs_raw = sum(r['total_imagenes'] for r in resultados)
    con_errores = sum(1 for r in resultados if r['errores'])

    print("\n" + "=" * 80)
    print(f"EXTRACCIÓN COMPLETADA en {total_ms / 1000:.1f}s")
    print(f"  Boletines: {len(resultados)}")
    print(f"  Páginas: {total_paginas:,}")
    print(f"  Caracteres: {total_chars:,}")
    print(f"  Imágenes: {total_imgs:,} útiles / {total_imgs_raw:,} totales")
    print(f"  Con errores: {con_errores}")
    print(f"  Salida: {dir_salida}")

    # Guardar índice global
    indice = {
        'fecha_extraccion': time.strftime('%Y-%m-%d %H:%M:%S'),
        'origen': dir_boletines,
        'total_boletines': len(resultados),
        'total_paginas': total_paginas,
        'total_chars': total_chars,
        'total_imagenes': total_imgs,
        'tiempo_total_ms': total_ms,
        'boletines': [{
            'numero': r['numero'],
            'era': r['era'],
            'tomos': len(r['tomos']),
            'paginas': r['total_paginas'],
            'chars': r['total_chars'],
            'imagenes': r['total_imagenes_filtradas'],
            'errores': len(r['errores']),
        } for r in resultados],
    }

    with open(os.path.join(dir_salida, "indice.json"), 'w', encoding='utf-8') as f:
        json.dump(indice, f, indent=2, ensure_ascii=False)

    print(f"\nÍndice guardado en: {os.path.join(dir_salida, 'indice.json')}")


if __name__ == '__main__':
    main()
