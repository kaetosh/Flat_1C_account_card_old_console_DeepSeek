# -*- coding: utf-8 -*-
import os
import sys
import subprocess
import tempfile
import shlex
from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Optional
from pathlib import Path
import pandas as pd
import numpy as np
from tqdm import tqdm
from colorama import init, Fore

from support_functions import fix_1c_excel_case, normalize_path, validate_paths, print_instruction_color, write_df_in_chunks, sort_columns
from custom_errors import NoExcelFilesFoundError, RegisterProcessingError, NoRegisterFilesFoundError, IncorrectFolderOrFilesPath

init(autoreset=True)

# Конфигурация столбцов
DESIRED_ORDER = {
    'upp': [
        'Имя_файла', 'Дата', 'Документ', 'Дебет', 'Дебет_значение', 'Кредит', 
        'Кредит_значение', 'Текущее сальдо', 'Текущее сальдо_значение', 
        'Операция_1', 'Операция_2', 'Операция_3', 'Операция_4', 'Операция_5', 
        'Операция_6', 'Дт_количество', 'Кт_количество', 'Дт_валюта', 
        'Дт_валютая_сумма', 'Кт_валюта', 'Кт_валютая_сумма'
    ],
    'not_upp': [
        'Имя_файла', 'Период', 'Дебет', 'Дебет_значение', 'Кредит', 
        'Кредит_значение', 'Текущее сальдо', 'Текущее сальдо_значение', 
        'Документ_1', 'Документ_2', 'Аналитика Дт_1', 'Аналитика Дт_2', 
        'Аналитика Дт_3', 'Аналитика Дт_4', 'Аналитика Кт_1', 'Аналитика Кт_2', 
        'Аналитика Кт_3', 'Аналитика Кт_4', 'Дебет_количество', 
        'Кредит_количество', 'Дебет_валюта', 'Дебет_валютное_количество', 
        'Кредит_валюта', 'Кредит_валютное_количество'
    ]
}

class FileProcessor(ABC):
    """Абстрактный базовый класс для обработчиков файлов"""
    
    @abstractmethod
    def process_file(self, file_path: Path) -> pd.DataFrame:
        pass

class UPPFileProcessor(FileProcessor):
    """Обработчик для файлов из 1С УПП"""
    
    @staticmethod
    def _fast_keep_first_unique_per_row(df: pd.DataFrame) -> pd.DataFrame:
        arr = df.values
        mask = np.ones_like(arr, dtype=bool)
        
        for i in range(arr.shape[0]):
            seen = set()
            for j in range(arr.shape[1]):
                val = arr[i, j]
                if pd.isna(val):
                    continue
                if val in seen:
                    mask[i, j] = False
                else:
                    seen.add(val)
        
        return pd.DataFrame(np.where(mask, arr, np.nan), columns=df.columns)
    
    @staticmethod
    def _process_dataframe_optimized(df: pd.DataFrame) -> pd.DataFrame:
        first_col = df.iloc[:, 0].astype(str).str.lower()
        date_row_idx = first_col.str.contains('дата').idxmax() if first_col.str.contains('дата').any() else None
        
        if date_row_idx is None:
            raise RegisterProcessingError(Fore.RED + 'Файл не является карточкой счета 1с.\n')
        
        df.columns = df.iloc[date_row_idx].str.strip()
        df = df.iloc[date_row_idx + 1:].copy()
        
        df['Дата'] = pd.to_datetime(df['Дата'], format='%d.%m.%Y', errors='coerce')
        
        mask = df['Документ'].notna()
        df.loc[mask, 'Документ'] = (
            df.loc[mask, 'Документ'] + '_end' + 
            df.loc[mask].groupby('Документ').cumcount().add(1).astype(str)
        )
        
        df['Дата'] = df['Дата'].ffill()
        df['Документ'] = df['Документ'].ffill()

        
        
        df.columns = [
            f'NoNameCol {i+1}' if pd.isna(col) or col == '' else col 
            for i, col in enumerate(df.columns)
        ]

        df = df[df['Дата'].notna()].copy()
        return df.dropna(how='all', axis=0).dropna(how='all', axis=1)
    
    def _extract_special_data(
        self, 
        df: pd.DataFrame, 
        operation_filter: str, 
        cols_to_extract: List[str]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Извлекает данные по количеству и валюте"""
        filtered = df[df['Операция'] == operation_filter].copy()
        if filtered.empty:
            return pd.DataFrame(), df
        
        # Для количества
        if operation_filter == 'Кол-во':
            dt_col = pd.to_numeric(filtered['Дебет'], errors='coerce').fillna(0)
            kt_col = pd.to_numeric(filtered['Кредит'], errors='coerce').fillna(0)
            result = filtered[['Документ']].copy()
            result['Дт_количество'] = dt_col
            result['Кт_количество'] = kt_col
            return result, df[df['Операция'] != operation_filter]
        
        # Для валюты
        filtered.replace([np.nan, '\n', '\t', ' '], '', inplace=True)
        filtered.replace(r'^\s+$', '', inplace=True, regex=True)
        
        result = filtered[['Документ']].copy()
        result['Дт_валюта'] = filtered['Дебет']
        result['Дт_валютая_сумма'] = filtered.iloc[:, filtered.columns.get_loc('Дебет') + 1]
        result['Кт_валюта'] = filtered['Кредит']
        result['Кт_валютая_сумма'] = filtered.iloc[:, filtered.columns.get_loc('Кредит') + 1]
        
        return result, df[df['Операция'] != operation_filter]
    
    def process_file(self, file_path: Path) -> pd.DataFrame:
        fixed_data = fix_1c_excel_case(file_path)
        df = pd.read_excel(fixed_data, header=None)
        df = df.dropna(axis=1, how='all')
        df = self._process_dataframe_optimized(df)
        
        # Извлечение данных по количеству и валюте
        df_count, df = self._extract_special_data(df, 'Кол-во', ['Документ', 'Дт_количество', 'Кт_количество'])
        df_currency, df = self._extract_special_data(df, 'В валюте :', ['Документ', 'Дт_валюта', 'Дт_валютая_сумма', 'Кт_валюта', 'Кт_валютая_сумма'])
        
        if df.empty:
            raise RegisterProcessingError(Fore.RED + f"Карточка 1с пустая в файле {file_path.name}, обработка невозможна.\n")
        
        # Подготовка данных
        operations_pivot = (
            df.assign(row_num=df.groupby(['Дата', 'Документ']).cumcount() + 1)
            .pivot_table(index=['Дата', 'Документ'], columns='row_num', values='Операция', aggfunc='first')
            .reset_index()
            .rename(columns=lambda x: f'Операция_{x}' if isinstance(x, int) else x)
        )
        
        operations_pivot = self._fast_keep_first_unique_per_row(operations_pivot)
        operations_pivot = operations_pivot.dropna(how='all', axis=0).dropna(how='all', axis=1)
        
        doc_attributes = (
            df.drop_duplicates(subset=['Документ', 'Дебет', 'Кредит'])
            .set_index('Документ')
            .drop(columns=['Дата'])
        )
        
        # Объединение данных
        result = doc_attributes.join(operations_pivot.set_index('Документ'), how='left')
        
        for df_special in [df_count, df_currency]:
            if not df_special.empty:
                result = result.join(df_special.set_index('Документ'), how='left')
        
        result = result.reset_index()
        
        # Обработка колонок
        result = result.dropna(subset=['Дебет', 'Кредит'], how='all')
        result['Документ'] = result['Документ'].str.replace(r'_end\d+$', '', regex=True)
        result = result.dropna(how='all', axis=0).dropna(how='all', axis=1)
        
        cols = result.columns.tolist()
        #new_columns = [
        #    f'{cols[i - 1]}_значение' if str(col).startswith("NoNameCol") and i > 0 
        #    else ('NoNameCol0' if i == 0 else col)
        #    for i, col in enumerate(result.columns)
        #]
        new_columns = []
        for i, col in enumerate(cols):
            if str(col).startswith("NoNameCol"):
                if i == 0:
                    # Для первого столбца слева нет предыдущего, можно задать дефолтное имя
                    new_name = 'NoNameCol0'
                else:
                    left_col = cols[i - 1]
                    new_name = f'{left_col}_значение'
                new_columns.append(new_name)
            else:
                new_columns.append(col)
        result.columns = new_columns
        result.to_excel('001.xlsx')
        # Реорганизация колонок
        if 'Дата' in result.columns:
            result = result[['Дата'] + [col for col in result.columns if col != 'Дата']]
        
        result = result.drop(columns=['Операция'], errors='ignore')
        result.insert(0, 'Имя_файла', os.path.basename(file_path))
        
        return result

class NonUPPFileProcessor(FileProcessor):
    """Обработчик для файлов из 1С (не УПП)"""

    @staticmethod
    def _split_and_expand(df: pd.DataFrame, col_name: str, prefix: str) -> None:
        """Разбивает столбец с разделителем \n"""
        if col_name not in df.columns:
            return
            
        new_cols = df[col_name].str.split('\n', expand=True)
        if new_cols is None or new_cols.empty:
            df.drop(columns=[col_name], inplace=True)
            return
            
        new_cols.columns = [f'{prefix}_{i+1}' for i in range(new_cols.shape[1])]
        df[new_cols.columns] = new_cols
        df.drop(columns=[col_name], inplace=True)

    def _extract_special_data(
        self,
        df: pd.DataFrame,
        value_filter: str,
        columns: List[str]
    ) -> pd.DataFrame:
        """Извлекает специальные данные (количество/валюта)"""
        col_name = next((col for col in df.columns if str(col).startswith('Показ')), None)
        if not col_name:
            return pd.DataFrame()
            
        filtered = df[df[col_name] == value_filter].iloc[1:-1].copy()
        if filtered.empty:
            return pd.DataFrame()
            
        result = pd.DataFrame()
        
        try:
            if 'Дебет' in filtered.columns:
                dt_index = filtered.columns.get_loc('Дебет')
                if value_filter == 'Кол.':
                    result['Дебет_количество'] = pd.to_numeric(filtered.iloc[:, dt_index + 1], errors='coerce').fillna(0)
                elif value_filter == 'Вал.':
                    result['Дебет_валюта'] = filtered.iloc[:, dt_index + 1]
                    result['Дебет_валютное_количество'] = pd.to_numeric(filtered.iloc[:, dt_index + 2], errors='coerce').fillna(0)
        except (KeyError, IndexError):
            pass
            
        try:
            if 'Кредит' in filtered.columns:
                kt_index = filtered.columns.get_loc('Кредит')
                if value_filter == 'Кол.':
                    result['Кредит_количество'] = pd.to_numeric(filtered.iloc[:, kt_index + 1], errors='coerce').fillna(0)
                elif value_filter == 'Вал.':
                    result['Кредит_валюта'] = filtered.iloc[:, kt_index + 1]
                    result['Кредит_валютное_количество'] = pd.to_numeric(filtered.iloc[:, kt_index + 2], errors='coerce').fillna(0)
        except (KeyError, IndexError):
            pass
            
        return result

    def process_file(self, file_path: Path) -> pd.DataFrame:
        fixed_data = fix_1c_excel_case(file_path)
        df = pd.read_excel(fixed_data, header=None)
        df = df.dropna(axis=1, how='all')
        
        # Поиск строки с заголовками
        period_rows = df.index[df.iloc[:, 0] == 'Период'].tolist()
        if not period_rows:
            raise RegisterProcessingError(Fore.RED + 'Не найден заголовок Период в шапке таблицы')
            
        header_row = period_rows[0]
        df.columns = df.iloc[header_row]
        df = df.iloc[header_row + 1:].reset_index(drop=True)
        
        # Извлечение специальных данных
        df_count = self._extract_special_data(df, 'Кол.', ['Дебет_количество', 'Кредит_количество'])
        df_currency = self._extract_special_data(df, 'Вал.', ['Дебет_валюта', 'Дебет_валютное_количество', 'Кредит_валюта', 'Кредит_валютное_количество'])
        
        # Фильтрация по дате
        df['Период'] = pd.to_datetime(df['Период'], format='%d.%m.%Y', errors='coerce')
        df = df[df['Период'].notna()].copy().reset_index(drop=True)
        
        # Добавление извлеченных данных
        if not df_count.empty and len(df_count) == len(df):
            df = pd.concat([df, df_count], axis=1)
        if not df_currency.empty and len(df_currency) == len(df):
            df = pd.concat([df, df_currency], axis=1)
        
        # Обработка колонок
        for col in ['Документ', 'Аналитика Дт', 'Аналитика Кт']:
            self._split_and_expand(df, col, col.split()[0])
        cols = df.columns.tolist()
        new_columns = [
            f'{cols[i - 1]}_значение' if pd.isna(col) or col == '' and i > 0 
            else ('NoNameCol0' if i == 0 else col)
            for i, col in enumerate(df.columns)
        ]
        df.columns = new_columns
        
        df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
        df.insert(0, 'Имя_файла', os.path.basename(file_path))
        
        if df.empty:
            raise RegisterProcessingError(Fore.RED + f"Карточка 1с пустая в файле {file_path.name}, обработка невозможна. Файл не УПП\n")
        
        return df

class FileProcessorFactory:
    """Фабрика для создания обработчиков файлов"""
    
    @staticmethod
    def get_processor(file_path: Path) -> FileProcessor:
        fixed_data = fix_1c_excel_case(file_path)
        df = pd.read_excel(fixed_data, header=None, nrows=50)
        
        for _, row in df.iterrows():
            row_str = [str(cell).strip().lower() for cell in row]
            if 'дата' in row_str and 'документ' in row_str and 'операция' in row_str:
                return UPPFileProcessor()
            if 'период' in row_str and 'аналитика дт' in row_str and 'аналитика кт' in row_str:
                return NonUPPFileProcessor()
        
        raise RegisterProcessingError(f"Файл {file_path.name} не является корректной Карточкой счета из 1С.\n")

class ExcelValidator:
    @staticmethod
    def is_valid_excel(file_path: Path) -> bool:
        return file_path.suffix.lower() == '.xlsx'

class FileHandler:
    def __init__(self, verbose: bool = True):
        self.validator = ExcelValidator()
        self.processor_factory = FileProcessorFactory()
        self.verbose = verbose
        self.not_correct_files = []
        self.storage_processed_registers = {}
    
    def handle_input(self, input_path: Path) -> None:
        if input_path.is_file():
            self._process_single_file(input_path)
        elif input_path.is_dir():
            self._process_directory(input_path)
    
    def _process_single_file(self, file_path: Path) -> None:
        if not self.validator.is_valid_excel(file_path):
            self.not_correct_files.append(file_path.name)
            return

        try:
            processor = self.processor_factory.get_processor(file_path)
            if self.verbose:
                print('Файл в обработке...', end='\r')
            result = processor.process_file(file_path)
            self.storage_processed_registers[file_path.name] = result
        except RegisterProcessingError as e:
            self.not_correct_files.append(file_path.name)
    
    def _process_directory(self, dir_path: Path) -> None:
        original_verbose = self.verbose
        self.verbose = False
        
        try:
            excel_files = self._get_excel_files(dir_path)
            upp_results = []
            non_upp_results = []

            for file_path in tqdm(excel_files, desc="Обработка файлов"):
                try:
                    processor = self.processor_factory.get_processor(file_path)
                    result = processor.process_file(file_path)
                    
                    if isinstance(processor, UPPFileProcessor):
                        upp_results.append(result)
                    else:
                        non_upp_results.append(result)
                except Exception:
                    self.not_correct_files.append(file_path.name)
            
            # Обработка результатов
            df_pivot_upp = sort_columns(
                pd.concat(upp_results), 
                DESIRED_ORDER['upp']
            ) if upp_results else pd.DataFrame()
            
            df_pivot_non_upp = sort_columns(
                pd.concat(non_upp_results), 
                DESIRED_ORDER['not_upp']
            ) if non_upp_results else pd.DataFrame()
            
            if not upp_results and not non_upp_results:
                raise NoRegisterFilesFoundError(Fore.RED + 'В папке не найдены карточки счета 1С')
            
            self._save_combined_results(df_pivot_upp, df_pivot_non_upp)
        finally:
            self.verbose = original_verbose
    
    @staticmethod
    def _get_excel_files(dir_path: Path) -> List[Path]:
        files = [f for f in dir_path.iterdir() if f.is_file() and f.suffix.lower() == '.xlsx']
        if not files:
            raise NoExcelFilesFoundError(Fore.RED + "В папке нет файлов Excel.")
        return files

    def _save_and_open_batch_result(self) -> None:
        if not self.storage_processed_registers:
            return
            
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_filename = tmp.name

        with pd.ExcelWriter(temp_filename, engine='openpyxl') as writer:
            for sheet_name, df in self.storage_processed_registers.items():
                safe_name = sheet_name[:31]
                df.to_excel(writer, sheet_name=safe_name, index=False)
        
        if sys.platform == "win32":
            os.startfile(temp_filename)
        elif sys.platform == "darwin":
            subprocess.run(["open", temp_filename])
        else:
            subprocess.run(["xdg-open", temp_filename])

    @staticmethod
    def _save_combined_results(df_upp: pd.DataFrame, df_non_upp: pd.DataFrame) -> None:
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_filename = tmp.name

        with pd.ExcelWriter(temp_filename, engine='openpyxl') as writer:
            if not df_upp.empty:
                write_df_in_chunks(writer, df_upp, 'UPP')
            if not df_non_upp.empty:
                write_df_in_chunks(writer, df_non_upp, 'Non_UPP')
        
        if sys.platform == "win32":
            os.startfile(temp_filename)
        elif sys.platform == "darwin":
            subprocess.run(["open", temp_filename])
        else:
            subprocess.run(["xdg-open", temp_filename])

class UserInterface:
    @staticmethod
    def get_input() -> List[Path]:
        print(Fore.YELLOW + "\nПеретащите файл карточки 1С (.xlsx) или папку и нажмите Enter:")
        input_str = input().strip().replace('\\', '/')
        paths = [Path(p) for p in shlex.split(input_str)]
        
        if not validate_paths(paths):
            raise IncorrectFolderOrFilesPath(Fore.RED + 'Неверные пути к папке или файлу/файлам.')
        return paths

def main():
    print_instruction_color()
    ui = UserInterface()
    file_handler = FileHandler()

    while True:
        try:
            input_paths = ui.get_input()
            for input_path in input_paths:
                try:
                    file_handler.handle_input(normalize_path(input_path))
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    print(f"{e}")
                    if input_path.is_file():
                        file_handler.not_correct_files.append(input_path.name)
            
            if file_handler.storage_processed_registers:
                file_handler._save_and_open_batch_result()
        except KeyboardInterrupt:
            print("\nПрограмма прервана пользователем.")
            break
        except Exception as e:
            print(f"{e}")
        finally:
            # Вывод информации о неправильных файлах
            if file_handler.not_correct_files:
                print(Fore.RED + 'Файлы не распознаны как Карточки счета 1С:')
                for file_name in file_handler.not_correct_files:
                    print(Fore.RED + f"  - {file_name}")
                file_handler.not_correct_files.clear()
            
            # Очистка хранилища
            if file_handler.storage_processed_registers:
                file_handler.storage_processed_registers.clear()

if __name__ == "__main__":
    main()