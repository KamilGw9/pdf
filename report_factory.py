# -*- coding: utf-8 -*-
"""
ReportFactory - ładuje konfigurację z YAML i generuje raporty PDF.
Obsługuje CALL procedura(param) jako źródło danych.
Jeden numer = jeden PDF. Zmiana numeru = nowy plik PDF.
"""

import yaml
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
import os

from main import (
    get_hana_connection,
    get_output_folder,
    save_generation_log,
    generate_pdf,
    Timer,
    FONT_REGULAR,
    FONT_BOLD,
    TEXT_COLOR,
    BASE_OUTPUT_DIR,
)

from report_types import ReportType, TableConfig, ReportDataV2


class ReportFactory:
    """
    Factory do generowania raportów z dynamiczną liczbą tabel.
    Konfiguracja z plików YAML, dane z SAP HANA (CALL procedura).
    """

    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.configs: Dict[ReportType, dict] = {}
        self._load_all_configs()

    def _load_all_configs(self):
        """Ładuje wszystkie konfiguracje YAML."""
        print(f"\n📂 Konfiguracje: {self.config_dir.resolve()}")
        for report_type in ReportType:
            config_path = self.config_dir / f"{report_type.value}.yaml"
            if not config_path.exists():
                print(f"  ⚠️  Brak: {config_path.name}")
                continue
            with open(config_path, 'r', encoding='utf-8') as f:
                self.configs[report_type] = yaml.safe_load(f)
            tables = len(self.configs[report_type].get('data_tables', []))
            print(f"  ✓ {report_type.value} ({tables} tabel)")

    # ------------------------------------------------------------------
    #  POBIERANIE DANYCH Z HANA
    # ------------------------------------------------------------------

    def _execute_query(self, query: str, nr: int) -> List[List[str]]:
        """
        Wykonuje CALL schema.procedura(nr) i zwraca result set.
        Parametr {nr} w zapytaniu zostanie podstawiony wartością.
        """
        connection = get_hana_connection()
        cursor = connection.cursor()
        try:
            resolved_query = query.replace('{nr}', str(nr)).strip()
            print(f"    SQL: {resolved_query}")
            cursor.execute(resolved_query)

            try:
                rows = cursor.fetchall()
                return [
                    [str(cell) if cell is not None else '' for cell in row]
                    for row in rows
                ]
            except Exception:
                return []
        finally:
            cursor.close()
            connection.close()

    def _fetch_summary(self, config: dict, nr: int) -> List[List[str]]:
        """Pobiera dane podsumowania (pary etykieta/wartość)."""
        query = config['summary']['query']
        labels = config['summary']['labels']
        rows = self._execute_query(query, nr)

        if not rows:
            return [[label, ''] for label in labels]

        # Pierwszy wiersz wyników → mapuj na etykiety
        values = rows[0]
        return [
            [label, values[i] if i < len(values) else '']
            for i, label in enumerate(labels)
        ]

    def _fetch_table_data(self, table_cfg: dict, nr: int) -> List[List[str]]:
        """Pobiera dane dla pojedynczej tabeli."""
        return self._execute_query(table_cfg['query'], nr)

    # ------------------------------------------------------------------
    #  POBIERANIE LISTY KLIENTÓW / NUMERÓW
    # ------------------------------------------------------------------

    def get_clients_list(self) -> List[int]:
        """
        Pobiera listę unikalnych numerów klientów z HANA.
        UZUPEŁNIJ: zapytanie lub procedurę zwracającą listę NR.
        """
        connection = get_hana_connection()
        cursor = connection.cursor()
        try:
            # UZUPEŁNIJ: np. CALL SCHEMA.PROC_LISTA_KLIENTOW()
            cursor.execute("CALL SCHEMA.PROC_LISTA_KLIENTOW()")
            rows = cursor.fetchall()
            return [int(row[0]) for row in rows if row]
        finally:
            cursor.close()
            connection.close()

    # ------------------------------------------------------------------
    #  GENEROWANIE RAPORTÓW
    # ------------------------------------------------------------------

    def generate_report(self, report_type: ReportType, nr: int) -> str:
        """
        Generuje pojedynczy raport PDF dla danego numeru.

        Args:
            report_type: Typ raportu (enum)
            nr: Numer klienta (parametr do CALL procedura)

        Returns:
            Ścieżka do wygenerowanego PDF
        """
        if report_type not in self.configs:
            raise ValueError(f"Brak konfiguracji: {report_type.value}")

        config = self.configs[report_type]
        print(f"\n→ {config['report_type']} | NR={nr}")

        # Pobierz podsumowanie
        with Timer("  Podsumowanie"):
            summary_data = self._fetch_summary(config, nr)

        # Nazwa klienta z pierwszego wiersza podsumowania (jeśli jest)
        client_name = summary_data[0][1] if summary_data and len(summary_data[0]) > 1 else str(nr)

        # Pobierz dane tabel
        tables = []
        for table_cfg in config['data_tables']:
            with Timer(f"  {table_cfg['title']}"):
                data = self._fetch_table_data(table_cfg, nr)
                fmt = table_cfg.get('formatting', {})
                tables.append({
                    'title': table_cfg['title'],
                    'headers': table_cfg['headers'],
                    'data': data,
                    'currency_columns': fmt.get('currency_columns', []),
                    'percentage_columns': fmt.get('percentage_columns', []),
                    'add_total_row': fmt.get('add_total_row', True),
                    'column_widths': fmt.get('column_widths'),
                })

        # Generuj PDF — każdy NR = osobny plik
        filename = f"{report_type.value}_{nr}.pdf"
        subfolder = report_type.value.replace('_', '#')

        with Timer(f"  PDF ({len(tables)} tabel)"):
            pdf_path = generate_pdf(
                client_name=client_name,
                summary_title=config['summary']['title'],
                summary_data=summary_data,
                tables=tables,
                filename=filename,
                subfolder=subfolder,
            )

        return pdf_path

    def generate_all_for_nr(self, nr: int,
                            report_types: List[ReportType] = None) -> List[str]:
        """
        Generuje wybrane (lub wszystkie) typy raportów dla numeru.

        Args:
            nr: Numer klienta
            report_types: Lista typów (None = wszystkie skonfigurowane)

        Returns:
            Lista ścieżek do wygenerowanych PDF
        """
        types = report_types or list(self.configs.keys())
        generated = []

        for rt in types:
            if rt not in self.configs:
                print(f"  ⚠️  Pomijam {rt.value} - brak konfiguracji")
                continue
            try:
                path = self.generate_report(rt, nr)
                generated.append(path)
            except Exception as e:
                print(f"  ✗ {rt.value}: {e}")

        return generated

