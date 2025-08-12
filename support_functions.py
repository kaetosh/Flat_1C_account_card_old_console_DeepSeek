# -*- coding: utf-8 -*-
'''
Оптимизированная версия support_functions
'''

import zipfile
from io import BytesIO
from pathlib import Path
from typing import List, Tuple, Optional, Iterable
import math
import re
from colorama import init, Fore, Style
import pandas as pd

init(autoreset=True)

# Глобальные константы
MAX_EXCEL_ROWS = 1_000_000
COLUMN_PREFIXES = ('Операция_', 'Документ_', 'Аналитика Дт_', 'Аналитика Кт_')

def sort_columns(df: pd.DataFrame, desired_order: List[str]) -> pd.DataFrame:
    """Сортирует столбцы DataFrame в заданном порядке с группировкой по префиксам"""
    cols = df.columns.tolist()
    
    # 1. Фиксированные колонки (не имеющие числового суффикса)
    fixed_cols = [
        col for col in desired_order 
        if col in cols and not any(col.startswith(prefix) for prefix in COLUMN_PREFIXES)
    ]

    # 2. Группируем колонки по префиксам
    grouped_cols = {}
    for prefix in COLUMN_PREFIXES:
        prefix_cols = [col for col in cols if col.startswith(prefix)]
        grouped_cols[prefix] = sorted(
            prefix_cols, 
            key=lambda x: int(re.search(rf"{re.escape(prefix)}(\d+)", x).group(1) or 0)
        )
    
    # 3. Собираем все колонки в порядке:
    #    - фиксированные
    #    - сгруппированные по префиксам
    #    - остальные
    ordered_cols = fixed_cols[:]
    for prefix in COLUMN_PREFIXES:
        ordered_cols.extend(grouped_cols.get(prefix, []))
    
    # 4. Добавляем колонки, не вошедшие в группы
    other_cols = set(cols) - set(ordered_cols)
    ordered_cols.extend(other_cols)
    
    return df[ordered_cols]

def write_df_in_chunks(
    writer: pd.ExcelWriter,
    df: pd.DataFrame,
    base_sheet_name: str,
    max_rows: int = MAX_EXCEL_ROWS
) -> None:
    """Записывает DataFrame в Excel частями с учетом ограничения на строки"""
    if df.empty:
        return
        
    n_chunks = math.ceil(len(df) / max_rows)
    
    for i in range(n_chunks):
        start = i * max_rows
        end = min((i + 1) * max_rows, len(df))
        sheet_name = f"{base_sheet_name}{i + 1}" if n_chunks > 1 else base_sheet_name
        
        df.iloc[start:end].to_excel(
            writer,
            sheet_name=sheet_name[:31],  # Ограничение длины имени листа
            index=False
        )

def print_instruction_color() -> None:
    """Выводит цветную инструкцию по использованию программы"""
    title = "Обработчик Карточки счета 1С"
    subtitle = "Формирует плоскую таблицу из регистров 1С"
    
    header = f"{Fore.CYAN}{'=' * 60}\n{title.center(60)}\n{subtitle.center(60)}\n{'=' * 60}{Style.RESET_ALL}"
    
    sections = [
        (
            Fore.YELLOW + "Режимы работы:", 
            [
                "1) Обработка регистров по отдельности:",
                "   - Перетягивайте файл в окно программы",
                "   - Результат откроется в отдельном Excel-файле\n",
                "2) Пакетная обработка (сводная таблица):",
                "   - Перетягивайте папку с файлами",
                "   - Результаты будут в одном Excel-файле\n"
            ]
        ),
        (
            Fore.YELLOW + "Поддерживаемые версии 1С:", 
            [
                "1) Управление производственным предприятием (1С 8.3):",
                "   - Заголовки: |Дата|Документ|Операция|\n",
                "2) Бухгалтерия предприятия, ERP Агропромышленный комплекс,",
                "   ERP Управление предприятием 2:",
                "   - Заголовки: |Период|Документ|Аналитика Дт|Аналитика Кт|\n"
            ]
        ),
        (
            Fore.YELLOW + "Особенности:", 
            [
                "- Результаты сохраняются на листах UPP и Non_UPP",
                "- Файлы >1 млн строк разбиваются на части",
                "- Время обработки: 500 тыс. строк ~ 2 мин\n"
            ]
        )
    ]
    
    print(header)
    for header, content in sections:
        print(header)
        for line in content:
            print(Style.RESET_ALL + line)

def validate_paths(paths: Iterable[Path]) -> bool:
    """Проверяет валидность путей"""
    if not paths:
        return False
        
    return all(
        p.resolve().exists() 
        for p in paths
    )

def fix_1c_excel_case(file_path: Path) -> BytesIO:
    """Исправляет регистр имен в xlsx-архивах 1С"""
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            new_zip = BytesIO()
            
            with zipfile.ZipFile(new_zip, 'w') as new_z:
                for item in z.infolist():
                    # Исправляем только проблемные имена
                    new_name = (
                        'xl/sharedStrings.xml' 
                        if item.filename == 'xl/SharedStrings.xml' 
                        else item.filename
                    )
                    new_z.writestr(new_name, z.read(item))
        
        new_zip.seek(0)
        return new_zip
        
    except PermissionError as e:
        raise PermissionError(
            f"Файл {file_path.name} открыт в другой программе. Закройте его."
        ) from e
    except Exception as e:
        raise RuntimeError(
            f"Ошибка обработки файла {file_path.name}: {str(e)}"
        ) from e

def normalize_path(path_str) -> Path:
    # Если path_str — объект Path, преобразуем в строку
    path_str = str(path_str)
    normalized_str = path_str.replace('\\', '/').replace('-', '—')
    return Path(normalized_str)
