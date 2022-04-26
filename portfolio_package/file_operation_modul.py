from dataclasses import dataclass
import xlrd, xlwt, openpyxl


@dataclass()
class File:
    path: str = ''
    name_file: str = ''

    # получить количество строк в файле
    def GetAmountRows(self, name_list: str):
        # полное имя
        filename = self.path + '\\' + self.name_file
        # открыть файл
        wb = openpyxl.load_workbook(filename=filename)
        # выбор листа
        sheet = wb[name_list]
        return sheet.max_row + 1

    # прочитать столбец из файла
    def ReadColumnExcel(self, name_list: str, column_: int, start_row: int, end_row: int):
        # полное имя
        filename = self.path + '\\' + self.name_file

        # для формата Excel 2003 файлы .xls
        # # открыть файл
        # rb = xlrd.open_workbook(filename, formatting_info=True)
        # # выбор листа
        # sheet = rb.sheet_by_name(self.name_list)
        # vals = sheet.row_values(0)[0]
        # print(vals)
        # чтение диапазона
        # vals = [sheet.row_values(rownum) for rownum in range(sheet.nrows)]

        # для формата Excel 2003 файлы .xlsx
        # открыть файл
        wb = openpyxl.load_workbook(filename=filename)
        # выбор листа
        sheet = wb[name_list]
        # чтение ячейки
        # vals = sheet['A1'].value
        # чтение диапазона
        # rows - это список в ячейке 0 которого - номер строки с которого нужно начинать чтение
        # в ячейке 1 - номер последней строки
        result_list = []
        for i in range(start_row, end_row):
            # print(i, sheet.cell(row=i, column=column_).value)
            result_list.append(sheet.cell(row=i, column=column_).value)
        return result_list
