# -*- coding: utf-8 -*-
"""
Generator raportów PDF z danymi z SAP HANA.
Układ: Nagłówek + Podsumowanie (góra) + N dynamicznych tabel (dół).
Konfiguracja tabel w plikach YAML (folder config/).
"""

import os
import csv
import time
from datetime import datetime
from typing import List, Dict, Any
from reportlab.platypus import KeepTogether

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Spacer, Paragraph
)
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.styles import ParagraphStyle


# =========================================================================
#  KONFIGURACJA
# =========================================================================

# Przełącznik: True = dane mockowe, False = prawdziwa baza SAP HANA
# Aby przełączyć na produkcję: ustaw USE_MOCK = False i uzupełnij HANA_CONFIG
USE_MOCK = False

HANA_CONFIG = {
    'address': 'addres',       # UZUPEŁNIJ: np. 'hana-server.company.com'
    'port': port,       # UZUPEŁNIJ: np. 30015
    'user': 'user',          # UZUPEŁNIJ
    'password': 'Password',      # UZUPEŁNIJ
    #'schema': '',        # UZUPEŁNIJ: np. 'MY_SCHEMA'
    #'encrypt': True,
    #'sslValidateCertificate': False,
}

BASE_OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))


# =========================================================================
#  CZCIONKI I KOLORY
# =========================================================================

try:
    pdfmetrics.registerFont(TTFont('CenturyGothic', 'GOTHIC.TTF'))
    pdfmetrics.registerFont(TTFont('CenturyGothic-Bold', 'GOTHICB.TTF'))
    FONT_REGULAR = 'CenturyGothic'
    FONT_BOLD = 'CenturyGothic-Bold'
except Exception:
    FONT_REGULAR = 'Helvetica'
    FONT_BOLD = 'Helvetica-Bold'

HEADER_BG_COLOR = colors.HexColor("#001E64")
HEADER_TEXT_COLOR = colors.white
ROW_ALT_COLOR = colors.HexColor("#FBFCFF")
BORDER_COLOR = colors.HexColor("#EBF1FF")
TEXT_COLOR = colors.HexColor("#001E64")


# =========================================================================
#  POŁĄCZENIE Z SAP HANA
# =========================================================================

def get_hana_connection():
    """Tworzy i zwraca połączenie z SAP HANA (lub mock)."""
    if USE_MOCK:
        from mock_hana import mock_dbapi
        return mock_dbapi.connect()

    from hdbcli import dbapi

    connection = dbapi.connect(
        address=HANA_CONFIG['address'],
        port=HANA_CONFIG['port'],
        user=HANA_CONFIG['user'],
        password=HANA_CONFIG['password'],
        #encrypt=HANA_CONFIG.get('encrypt', True),
        #sslValidateCertificate=HANA_CONFIG.get('sslValidateCertificate', False),
    )

    # schema = HANA_CONFIG.get('schema', '')
    # if schema:
    cursor = connection.cursor()
    #cursor.execute(f'SET SCHEMA "{schema}"')
    cursor.close()

    return connection


# =========================================================================
#  NARZĘDZIA
# =========================================================================

class Timer:
    """Context manager do mierzenia czasu operacji."""

    def __init__(self, name: str = "Operacja"):
        self.name = name
        self.elapsed = 0.0

    def __enter__(self):
        self._start = time.perf_counter()
        print(f"⏱️  {self.name}...")
        return self

    def __exit__(self, *args):
        self.elapsed = time.perf_counter() - self._start
        print(f"   → {self.elapsed:.3f}s")

    def formatted(self) -> str:
        if self.elapsed < 1:
            return f"{self.elapsed * 1000:.0f}ms"
        return f"{self.elapsed:.2f}s"


def get_output_folder(subfolder: str = None) -> str:
    """Zwraca ścieżkę do folderu wyjściowego (tworzy jeśli brak)."""
    today = datetime.now().strftime('%Y-%m-%d')
    if subfolder:
        folder_name = f"{today}#{subfolder}"
    else:
        folder_name = today
    output_dir = os.path.join(BASE_OUTPUT_DIR, folder_name)
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def get_output_path(filename: str, subfolder: str = None) -> str:
    """Pełna ścieżka do pliku w folderze wyjściowym."""
    return os.path.join(get_output_folder(subfolder), filename)


# =========================================================================
#  FORMATOWANIE WARTOŚCI
# =========================================================================

def is_numeric(value) -> bool:
    """Sprawdza czy wartość jest liczbą."""
    try:
        float(str(value).replace(',', '.').replace(' ', ''))
        return True
    except (ValueError, AttributeError):
        return False


def format_as_currency(value) -> str:
    """Formatuje wartość jako walutę PLN (np. '1 234,56 zł')."""
    try:
        num = float(str(value).replace(',', '.').replace(' ', ''))
        return f"{num:,.2f}".replace(',', ' ').replace('.', ',') + " zł"
    except (ValueError, AttributeError):
        return str(value)


def format_as_percentage(value) -> str:
    """Formatuje wartość jako procent."""
    try:
        num = float(str(value).replace(',', '.').replace(' ', ''))
        return f"{num:.2f}%"
    except (ValueError, AttributeError):
        return str(value)


def calculate_column_sum(data: List[List[str]], col_index: int) -> str:
    """Oblicza sumę wartości w kolumnie."""
    total = 0.0
    has_numbers = False
    for row in data:
        if col_index < len(row) and is_numeric(row[col_index]):
            total += float(str(row[col_index]).replace(',', '.').replace(' ', ''))
            has_numbers = True
    return str(total) if has_numbers else ''


def calculate_column_max(data: List[List[str]], col_index: int) -> str:
    """Oblicza MAX wartości w kolumnie (dla procentów)."""
    max_val = None
    for row in data:
        if col_index < len(row) and is_numeric(row[col_index]):
            val = float(str(row[col_index]).replace(',', '.').replace(' ', ''))
            if max_val is None or val > max_val:
                max_val = val
    return str(max_val) if max_val is not None else ''


# =========================================================================
#  STYLE TABEL
# =========================================================================

def get_summary_table_style() -> TableStyle:
    """Styl dla tabeli podsumowania."""
    return TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), HEADER_BG_COLOR),
        ('TEXTCOLOR', (0, 0), (-1, 0), HEADER_TEXT_COLOR),
        ('FONTNAME', (0, 0), (-1, 0), FONT_BOLD),
        ('FONTSIZE', (0, 0), (-1, 0), 6.5),
        ('ALIGN', (0, 0), (-1, 0), 'LEFT'),
        ('SPAN', (0, 0), (-1, 0)),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, ROW_ALT_COLOR]),
        ('TEXTCOLOR', (0, 1), (0, -1), TEXT_COLOR),
        ('FONTNAME', (0, 1), (0, -1), FONT_BOLD),
        ('FONTSIZE', (0, 1), (0, -1), 5.5),
        ('ALIGN', (0, 1), (0, -1), 'LEFT'),
        ('TEXTCOLOR', (1, 1), (1, -1), TEXT_COLOR),
        ('FONTNAME', (1, 1), (1, -1), FONT_REGULAR),
        ('FONTSIZE', (1, 1), (1, -1), 6),
        ('ALIGN', (1, 1), (1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOX', (0, 0), (-1, -1), 0.5, BORDER_COLOR),
        ('LINEBELOW', (0, 0), (-1, 0), 0.5, BORDER_COLOR),
        ('LEFTPADDING', (0, 0), (-1, -1), 4),
        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
        ('TOPPADDING', (0, 0), (-1, -1), 2),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
    ])


def get_large_table_style(has_total_row: bool = False, num_cols: int = 5) -> TableStyle:
    """Styl dla dużych tabel danych – dynamiczny rozmiar czcionki."""
    # Mniej kolumn → większa czcionka; 11 kolumn → mniejsza
    # if num_cols <= 5:
    #     title_fs, header_fs, data_fs = 8, 7, 6.5
    # elif num_cols <= 8:
    #     title_fs, header_fs, data_fs = 7, 6.5, 6
    # else:
    #     title_fs, header_fs, data_fs = 6.5, 5.5, 5.5
    title_fs, header_fs, data_fs = 7, 5.5, 5.5
    pad = 2 if num_cols > 8 else 3

    cmds = [
        ('BACKGROUND', (0, 0), (-1, 0), HEADER_BG_COLOR),
        ('TEXTCOLOR', (0, 0), (-1, 0), HEADER_TEXT_COLOR),
        ('FONTNAME', (0, 0), (-1, 0), FONT_BOLD),
        ('FONTSIZE', (0, 0), (-1, 0), title_fs),
        ('ALIGN', (0, 0), (-1, 0), 'LEFT'),
        ('SPAN', (0, 0), (-1, 0)),
        ('BACKGROUND', (0, 1), (-1, 1), colors.HexColor("#F7F9FF")),
        ('TEXTCOLOR', (0, 1), (-1, 1), TEXT_COLOR),
        ('FONTNAME', (0, 1), (-1, 1), FONT_BOLD),
        ('FONTSIZE', (0, 1), (-1, 1), header_fs),
        ('ALIGN', (0, 1), (-1, 1), 'CENTER'),
        ('TEXTCOLOR', (0, 2), (-1, -2 if has_total_row else -1), TEXT_COLOR),
        ('FONTNAME', (0, 2), (-1, -2 if has_total_row else -1), FONT_REGULAR),
        ('FONTSIZE', (0, 2), (-1, -2 if has_total_row else -1), data_fs),
        ('ALIGN', (0, 2), (-1, -2 if has_total_row else -1), 'RIGHT'),
        ('ALIGN', (0, 2), (0, -2 if has_total_row else -1), 'LEFT'),
        ('ROWBACKGROUNDS', (0, 2), (-1, -2 if has_total_row else -1),
         [colors.white, ROW_ALT_COLOR]),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOX', (0, 0), (-1, -1), 0.5, BORDER_COLOR),
        ('LEFTPADDING', (0, 0), (-1, -1), pad),
        ('RIGHTPADDING', (0, 0), (-1, -1), pad),
        ('TOPPADDING', (0, 0), (-1, -1), 2),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
    ]
    if has_total_row:
        cmds.extend([
            ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor("#E3E7ED")),
            ('TEXTCOLOR', (0, -1), (-1, -1), TEXT_COLOR),
            ('FONTNAME', (0, -1), (-1, -1), FONT_BOLD),
            ('FONTSIZE', (0, -1), (-1, -1), data_fs),
            ('ALIGN', (0, -1), (0, -1), 'LEFT'),
            ('ALIGN', (1, -1), (-1, -1), 'RIGHT'),
            ('LINEABOVE', (0, -1), (-1, -1), 1, colors.HexColor("#001E64")),
        ])
    return TableStyle(cmds)


# =========================================================================
#  BUDOWANIE TABEL
# =========================================================================

def create_summary_table(title: str, summary_data: List[List[str]]) -> Table:
    """Tworzy pionową tabelę podsumowania (tytuł + pary etykieta/wartość)."""
    label_style = ParagraphStyle(
        'SummaryLabel', fontName=FONT_BOLD, fontSize=6,
        textColor=TEXT_COLOR, leading=8,
    )
    cell_style = ParagraphStyle(
        'SummaryCell', fontName=FONT_REGULAR, fontSize=6,
        textColor=TEXT_COLOR, leading=8,
    )
    table_data = [[title, '']]
    for row in summary_data:
        label = row[0] if len(row) > 0 else ''
        value = row[1] if len(row) > 1 else ''
        table_data.append([
            Paragraph(str(label), label_style),
            Paragraph(str(value), cell_style),
        ])

    while len(table_data) < 9:
        table_data.append(['', ''])

    col_widths = [2.8 * cm, 5.0 * cm]
    # Auto row heights
    row_heights = [0.45 * cm] + [None] * (len(table_data) - 1)
    table = Table(table_data, colWidths=col_widths, rowHeights=row_heights)
    table.setStyle(get_summary_table_style())
    return table


def create_report_header(client_name: str) -> List:
    """Tworzy nagłówek raportu: tytuł + data."""
    title_style = ParagraphStyle(
        'ReportTitle', fontName=FONT_BOLD, fontSize=18,
        textColor=TEXT_COLOR, spaceAfter=2, leading=22,
    )
    date_style = ParagraphStyle(
        'ReportDate', fontName=FONT_REGULAR, fontSize=6,
        textColor=colors.HexColor("#666666"), spaceAfter=4,
    )
    return [
        Paragraph("Twoje bonusy", title_style),
        Paragraph(f"Data wygenerowania: {datetime.now().strftime('%d.%m.%Y')}", date_style),
    ]


def _compute_col_widths(num_cols: int, headers: List[str],
                        currency_columns: List[int],
                        currency_columns_2: List[int],
                        percentage_columns: List[int]) -> List[float]:
    """Inteligentnie rozkłada szerokości kolumn wg typu danych."""
    available = A4[0] - 2 * cm  # ~19.5 cm

    if num_cols <= 5:
        return [available / num_cols] * num_cols

    # Wagi: tekst=3, waluta=2, procent=1.2, reszta=1.5
    weights = []
    for i in range(num_cols):
        h = headers[i].lower() if i < len(headers) else ''
        if i in currency_columns:
            weights.append(2.0)
        elif i in currency_columns_2:
            weights.append(2.0)
        elif i in percentage_columns:
            weights.append(1.7)
        elif any(k in h for k in ['nazwa']):
            weights.append(3.0)
        elif any(k in h for k in ['                          ']):

            weights.append(2.0)
        elif any(k in h for k in ['                 ']):
            
            weights.append(1.7)
        elif  any(k in h for k in ['klasyfikacja', 'typ']):
            weights.append(2.0)
        elif any(k in h for k in ['grupa']):
            weights.append(1.8)
        else:
            weights.append(1.5)

    total_w = sum(weights)
    return [available * w / total_w for w in weights]


def _wrap_cell(value: str, style: ParagraphStyle) -> Paragraph:
    """Opakowuje wartość w Paragraph – zapewnia zawijanie tekstu."""
    return Paragraph(str(value), style)

def _wrap_header(value: str, style: ParagraphStyle) -> Paragraph:

    words = str(value).split(' ')
    safe = ' '.join(f'<nobr>{w}</nobr>' for w in words)
    return Paragraph(safe, style)



def create_large_table(title: str, headers: List[str], data: List[List[str]],
                       col_widths: List[float] = None,
                       add_total_row: bool = True,
                       currency_columns: List[int] = None,
                       currency_columns_2: List[int] = None,
                       percentage_columns: List[int] = None,
                       subtitle: str = None) -> Table:
    """Tworzy dużą tabelę danych z zawijaniem tekstu i auto-wysokością."""
    num_cols = len(headers)
    currency_columns = currency_columns or []
    percentage_columns = percentage_columns or []
    currency_columns_2 = currency_columns_2 or []

    if col_widths is None:
        col_widths = _compute_col_widths(
            num_cols, headers, currency_columns, percentage_columns,currency_columns_2
        )

    # --- Style Paragraph dla komórek ---
    if num_cols <= 5:
        hdr_fs, data_fs = 7, 6.5
    elif num_cols <= 8:
        hdr_fs, data_fs = 6.5, 6
    else:
        hdr_fs, data_fs = 5.5, 5.5

    header_para_style = ParagraphStyle(
        'TblHeader', fontName=FONT_BOLD, fontSize=hdr_fs,
        textColor=TEXT_COLOR, leading=hdr_fs + 2,
        alignment=1,  # CENTER
    )
    data_para_style = ParagraphStyle(
        'TblData', fontName=FONT_REGULAR, fontSize=data_fs,
        textColor=TEXT_COLOR, leading=data_fs + 2,
        alignment=2,  # RIGHT
    )
    data_left_style = ParagraphStyle(
        'TblDataLeft', fontName=FONT_REGULAR, fontSize=data_fs,
        textColor=TEXT_COLOR, leading=data_fs + 2,
        alignment=0,  # LEFT
    )
    total_para_style = ParagraphStyle(
        'TblTotal', fontName=FONT_BOLD, fontSize=data_fs,
        textColor=TEXT_COLOR, leading=data_fs + 2,
        alignment=2,  # RIGHT
    )

    # Kolumny tekstowe (wyrównanie do lewej)
    text_cols = set()
    for i in range(num_cols):
        if i not in currency_columns and i not in percentage_columns and i not in currency_columns_2:
            text_cols.add(i)

    # --- Wiersz tytułowy ---
    display_title = f"{title} – {subtitle}" if subtitle else title
    table_data = [[display_title] + [''] * (num_cols - 1)]

    # --- Nagłówki (zawinięte w Paragraph) ---
    table_data.append([
        #_wrap_cell(h, header_para_style) for h in headers
        _wrap_header(h, header_para_style) for h in headers
    ])

    # --- Dane ---
    for row in data:
        padded = (row + [''] * num_cols)[:num_cols]
        formatted = []
        for i, val in enumerate(padded):
            if i in currency_columns and is_numeric(val):
                txt = format_as_currency(val)
                formatted.append(_wrap_cell(txt, data_para_style))
            elif i in currency_columns_2 and is_numeric(val):
                txt = format_as_currency(val)
                formatted.append(_wrap_cell(txt, data_para_style))
            elif i in percentage_columns and is_numeric(val):
                txt = format_as_percentage(val)
                formatted.append(_wrap_cell(txt, data_para_style))
            elif i in text_cols:
                formatted.append(_wrap_cell(str(val), data_left_style))
            else:
                formatted.append(_wrap_cell(str(val), data_para_style))
        table_data.append(formatted)

    # --- Wiersz SUMA ---
    if add_total_row and data:
        total = [_wrap_cell('SUMA', ParagraphStyle(
            'TblTotalLabel', fontName=FONT_BOLD, fontSize=data_fs,
            textColor=TEXT_COLOR, leading=data_fs + 2, alignment=0,
        ))]
        for ci in range(1, num_cols):
            if ci in currency_columns_2:
                total.append('')
            elif ci in percentage_columns:
                s = calculate_column_max(data, ci)
                if s:
                    total.append(_wrap_cell(format_as_percentage(s), total_para_style))
                else:
                    total.append('')
            else:
                s = calculate_column_sum(data, ci)
                if s and ci in currency_columns:
                    total.append(_wrap_cell(format_as_currency(s), total_para_style))
                elif s:
                    total.append(_wrap_cell(s, total_para_style))
                else:
                    total.append('')
        table_data.append(total)

    if not data:
        for _ in range(3):
            table_data.append([''] * num_cols)

    # Auto row heights (None = oblicz automatycznie na podstawie zawartości)
    title_h = 0.55 * cm
    row_heights = [title_h] + [None] * (len(table_data) - 1)

    table = Table(table_data, colWidths=col_widths, rowHeights=row_heights)
    table.setStyle(get_large_table_style(
        has_total_row=add_total_row and bool(data),
        num_cols=num_cols,
    ))
    return table


# =========================================================================
#  GÓRNA SEKCJA
# =========================================================================

# =========================================================================
#  LOGO
# =========================================================================


def add_logo_to_elements(elements: List, logo_path: str = "logo.png") -> List:
    """
    Dodaje logo na początku dokumentu z pełną szerokością strony.
    
    Args:
        elements: Lista elementów dokumentu
        logo_path: Ścieżka do pliku logo
    
    Returns:
        Lista elementów z logo na początku
    """
    try:
        from reportlab.platypus import Image
        from PIL import Image as PILImage
        
        # Sprawdź czy plik istnieje
        if not os.path.exists(logo_path):
            print(f"⚠️  Logo nie znalezione: {logo_path}")
            return elements
        
        # Pobierz rzeczywiste wymiary logo
        img = PILImage.open(logo_path)
        img_width, img_height = img.size
        
        # Oblicz proporcje
        aspect_ratio = img_height / img_width
        
        # Szerokość logo = szerokość strony minus marginesy
        page_width = A4[0] - 2 * cm  # ~19.5 cm
        logo_width = page_width
        logo_height = logo_width * aspect_ratio
        
        # Utwórz obiekt Image z obliczonymi wymiarami
        logo = Image(logo_path, width=logo_width, height=logo_height)
        logo.hAlign = 'CENTER'
        
        # Dodaj logo na początku
        result = [logo, Spacer(1, 0.3 * cm)]
        result.extend(elements)
        
        return result
        
    except Exception as e:
        print(f"⚠️  Błąd dodawania logo: {e}")
        return elements



def create_top_section(client_name: str, summary_title: str,
                       summary_data: List[List[str]]) -> List:
    """Tworzy górną sekcję: nagłówek po lewej, podsumowanie po prawej."""
    header_elems = create_report_header(client_name)
    header_table = Table([[header_elems[0]], [header_elems[1]]])
    header_table.setStyle(TableStyle([
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
    ]))

    summary = create_summary_table(summary_title, summary_data)
    page_width = A4[0] - 2 * cm

    container = Table(
        [[header_table, summary]],
        colWidths=[page_width * 0.52, page_width * 0.48],
    )
    container.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('ALIGN', (0, 0), (0, 0), 'LEFT'),
        ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ]))
    return [container]


# =========================================================================
#  GENEROWANIE PDF
# =========================================================================

def generate_pdf(client_name: str, summary_title: str,
                 summary_data: List[List[str]],
                 tables: List[Dict[str, Any]],
                 filename: str = "raport.pdf",
                 subfolder: str = None,
                 logo_path: str = None) -> str:
    """
    Generuje PDF z dynamiczną liczbą tabel.

    Args:
        client_name: Nazwa klienta (do nagłówka)
        summary_title: Tytuł tabeli podsumowania
        summary_data: Dane podsumowania [[etykieta, wartość], ...]
        tables: Lista słowników opisujących tabele:
                [{'title', 'headers', 'data', 'currency_columns',
                  'percentage_columns', 'add_total_row', 'column_widths'}, ...]
        filename: Nazwa pliku PDF
        subfolder: Opcjonalny podfolder wyjściowy

    Returns:
        Ścieżka do wygenerowanego pliku
    """
    output_path = get_output_path(filename, subfolder)

    doc = SimpleDocTemplate(
        output_path, pagesize=A4,
        leftMargin=1 * cm, rightMargin=1 * cm,
        topMargin=0.5 * cm, bottomMargin=1 * cm,
    )

    elements = create_top_section(client_name, summary_title, summary_data)
    elements.append(Spacer(1, 0.4 * cm))

    for i, t in enumerate(tables):
        table = create_large_table(
            title=t['title'],
            headers=t['headers'],
            data=t['data'],
            col_widths=t.get('column_widths'),
            add_total_row=t.get('add_total_row', True),
            currency_columns=t.get('currency_columns', []),
            currency_columns_2=t.get('currency_columns_2', []),
            percentage_columns=t.get('percentage_columns', []),
            subtitle=t.get('subtitle', ''),
        )

        elements.append(KeepTogether(table))

        #elements.append(table)
        if i < len(tables) - 1:
            elements.append(Spacer(1, 0.3 * cm))
    if logo_path:
        elements = add_logo_to_elements(elements, logo_path)

    doc.build(elements)
    print(f"✓ PDF: {output_path} ({len(tables)} tabel)")
    return output_path


# =========================================================================
#  WORKER DLA MULTIPROCESSING
# =========================================================================

def _render_pdf_task(args):
    """
    Funkcja robocza dla ProcessPoolExecutor.
    Przyjmuje serializowalny payload (dict) i generuje PDF.
    Każdy proces importuje moduł niezależnie = bezpieczne.
    """
    payload, logo_path = args
    return generate_pdf(
        client_name=payload['client_name'],
        summary_title=payload['summary_title'],
        summary_data=payload['summary_data'],
        tables=payload['tables'],
        filename=payload['filename'],
        subfolder=payload['subfolder'],
        logo_path=logo_path,
    )


# =========================================================================
#  MAIN
# =========================================================================

if __name__ == "__main__":
    from report_factory import ReportFactory
    from report_types import ReportType

    print("=" * 60)
    print("  GENERATOR RAPORTÓW PDF")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Tryb: {'MOCK' if USE_MOCK else 'SAP HANA'}")
    print("=" * 60)
    
    LOGO_PATH = "logo.png"

    ACTIVE_TYPES = [
        ReportType.WYKONANIE_A_TYP_A_TYDZIEN,  # per umowa (wszystkie sklepy)
        ReportType.WYKONANIE_A_TYP_B_TYDZIEN,  # per lokalizacja (wybrany sklep)
        #ReportType.WYKONANIE_C_TYP_A_TYDZIEN,
    ]
    SINGLE_CONTRACT = None
    MAX_WORKERS = min(6, (os.cpu_count() or 2))  # procesy do renderowania PDF

    with Timer("Generowanie raportów") as total:

        factory = None
        try:
            factory = ReportFactory(config_dir="config")

            # Pobierz listę umów
            contracts = factory.get_contracts_list()

            if SINGLE_CONTRACT:
                contracts = [c for c in contracts if c ['nr_umowy'] == SINGLE_CONTRACT]
                if not contracts:
                    print("Nie znaleziono")
                    exit(0)

            print(f"\n📋 Umowy ({len(contracts)}):")
            for c in contracts:
                print(f"   {c['nr_umowy']}  {c['klient']}  [{c['podtyp_klient']}]  ID={c['id']}  PLA={c['pla']}")

            TYP_A_TYPES = [rt for rt in ACTIVE_TYPES if rt in (
                ReportType.WYKONANIE_A_TYP_A_TYDZIEN,
                #ReportType.WYKONANIE_B_TYP_A_TYDZIEN
                #ReportType.WYKONANIE_C_TYP_A_TYDZIEN,
            )]

            TYP_B_TYPES = [rt for rt in ACTIVE_TYPES if rt in (
                ReportType.WYKONANIE_A_TYP_B_TYDZIEN,
                #ReportType.WYKONANIE_B_TYP_A_TYDZIEN
            )]

            all_generated = []
            pdf_tasks = []

            if TYP_A_TYPES:
                seen_nr = set()
                for c in contracts:
                    nr = c['nr_umowy']
                    if nr in seen_nr:
                        continue
                    seen_nr.add(nr)
                    for rt in TYP_A_TYPES:
                        if rt in factory.configs:
                            pdf_tasks.append((rt, nr, None, c['pla'], c['id']))

            # --- Typ B: 1 raport per lokalizacja (sklep) ---
            if TYP_B_TYPES:
                seen_nr_b = set()
                for c in contracts:
                    nr = c['nr_umowy']
                    if nr in seen_nr_b:
                        continue
                    seen_nr_b.add(nr)
                    locations = factory.get_locations_for_contract(nr)
                    print(f"  📍 {nr}: {len(locations)} lokalizacji")
                    for lok in locations:
                        for rt in TYP_B_TYPES:
                            if rt in factory.configs:
                                pdf_tasks.append((rt, nr, lok, c['pla'], c['id']))

            # =============================================================
            #  FAZA 1: BATCH PREFETCH — pobierz WSZYSTKIE dane z HANA
            # =============================================================
            factory.prepare_batch(ACTIVE_TYPES)

            # =============================================================
            #  FAZA 2: Budowanie payloadów (sekwencyjnie, używa cache)
            # =============================================================
            total_tasks = len(pdf_tasks)
            print(f"\n📦 Budowanie {total_tasks} payloadów...")

            payloads = []       # (payload, task_info) do renderowania
            csv_log_rows = []   # log do CSV

            for idx, (rt, nr, lok, pla, cid) in enumerate(pdf_tasks, 1):
                try:
                    payload = factory.build_report_payload(
                        rt, nr, lokalizacja=lok, pla=pla, contract_id=cid,
                    )
                    payloads.append((payload, (rt, nr, lok, pla, cid)))

                    # --- Zbierz dane do CSV (już tu, bo mamy pełne dane) ---
                    csv_row = {
                        'plik': payload['filename'],
                        'typ_raportu': rt.value,
                        'nr_umowy': nr,
                        'lokalizacja': lok or '',
                        'pla': pla or '',
                        'id': cid or '',
                        'klient': payload['client_name'],
                    }
                    for label, value in payload['summary_data']:
                        csv_row[f'summary_{label}'] = value
                    for table in payload['tables']:
                        tname = table['title']
                        sub = table.get('subtitle', '')
                        prefix = f"{tname}_{sub}" if sub else tname
                        headers = table['headers']
                        csv_row[f'{prefix}_wiersze'] = len(table['data'])
                        for row_idx, row_data in enumerate(table['data'], 1):
                            for col_idx, hdr in enumerate(headers):
                                val = row_data[col_idx] if col_idx < len(row_data) else ''
                                csv_row[f'{prefix}_w{row_idx}_{hdr}'] = val
                    csv_log_rows.append(csv_row)

                except Exception as e:
                    print(f"  X {rt.value} nr={nr}: {e}")
                    csv_log_rows.append({
                        'plik': 'BŁĄD',
                        'typ_raportu': rt.value,
                        'nr_umowy': nr,
                        'lokalizacja': lok or '',
                        'pla': pla or '',
                        'id': cid or '',
                        'klient': '',
                        'error': str(e),
                    })

            # Zamknij połączenie z bazą — dane już w pamięci
            factory.close()
            factory = None

            # =============================================================
            #  FAZA 3: Renderowanie PDF — równolegle (ProcessPoolExecutor)
            # =============================================================
            from concurrent.futures import ProcessPoolExecutor, as_completed

            print(f"\n⚡ Renderowanie {len(payloads)} PDF-ów ({MAX_WORKERS} procesów)...")

            render_args = [(p, LOGO_PATH) for p, _ in payloads]

            with ProcessPoolExecutor(max_workers=MAX_WORKERS) as pool:
                futures = {
                    pool.submit(_render_pdf_task, arg): payloads[i][1]
                    for i, arg in enumerate(render_args)
                }
                done_count = 0
                for future in as_completed(futures):
                    task_info = futures[future]
                    rt, nr, lok, pla, cid = task_info
                    try:
                        path = future.result(timeout=120)
                        all_generated.append(path)
                        done_count += 1
                        if done_count % 100 == 0:
                            print(f"   ✓ {done_count}/{len(payloads)} gotowych")
                    except Exception as e:
                        print(f"  X render {rt.value} nr={nr}: {e}")

            # --- Zapisz log CSV ---
            if csv_log_rows:
                csv_path = os.path.join(
                    get_output_folder(),
                    f"generation_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                )
                all_keys = []
                for row in csv_log_rows:
                    for k in row:
                        if k not in all_keys:
                            all_keys.append(k)
                with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=all_keys, delimiter=';')
                    writer.writeheader()
                    writer.writerows(csv_log_rows)
                print(f"\n📊 Log CSV: {csv_path}")

            print(f"\n{'─' * 50}")
            print(f"✓ Wygenerowano {len(all_generated)} raportów")
            for path in all_generated:
                print(f"   → {os.path.basename(path)}")
        except Exception as e:
            print(f"\n✗ Błąd: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if factory:
                factory.close()

    print(f"\n📁 Folder: {get_output_folder()}")
    print(f"⏱️  Czas: {total.formatted()}")

