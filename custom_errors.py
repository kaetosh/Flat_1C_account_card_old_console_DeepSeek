# -*- coding: utf-8 -*-
"""
Created on Fri Aug  1 09:39:30 2025

@author: a.karabedyan
"""

class NoExcelFilesFoundError(Exception):
    """Исключение вызывается, если в папке нет файлов Excel."""
    pass
class RegisterProcessingError(Exception):
    """Исключение вызывается, если регистр не является карточкой счета 1с или является пустой карточк"""
    pass
class PermissionFileExcelError(Exception):
    """Исключение вызывается, если файл Excel не доступен."""
    pass
class PathError(Exception):
    """Исключение вызывается, если указан несуществующий путь."""
    pass
class NoRegisterFilesFoundError(Exception):
    """Исключение вызывается, если в папке нет файлов Excel - Карточек счетов 1С."""
    pass
class IncorrectFolderOrFilesPath(Exception):
    """Исключение вызывается, если в консоль перетянули не путь к файлу/файлам или папке"""
    pass