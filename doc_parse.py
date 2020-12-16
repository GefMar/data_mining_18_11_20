from typing import List
import re
from pathlib import Path
import PyPDF2
from PyPDF2.utils import PdfReadError
from PIL import Image
import pytesseract


# todo Приведение структуры документов к плоскому списку


# todo Извлеч изображения из PDF
def pdf_image_extract(pdf_path: Path) -> List[Path]:
    result = []
    file_decode = {
        '/DCTDecode': 'jpg',
        '/FlateDecode': 'png',
        '/JPXDecode': 'jp2'
    }
    with open(pdf_path, 'rb') as file:
        try:
            pdf_file = PyPDF2.PdfFileReader(file)
        except PdfReadError as e:
            # todo записать в БД что файл битый
            return result
        
        for page_num, page in enumerate(pdf_file.pages, 1):
            image_data = page['/Resources']['/XObject']['/Im0']._data
            file_name = f"{pdf_path.name}.{page_num}.{file_decode[page['/Resources']['/XObject']['/Im0']['/Filter']]}"
            image_path = pdf_path.parent.joinpath(file_name)
            if save_image_to_file(image_path, image_data):
                result.append(image_path)
            else:
                # todo Записать в БД ошибку сохранения файла
                pass
    
    return result


def save_image_to_file(image_path, image_data):
    try:
        with open(image_path, 'wb') as file:
            file.write(image_data)
            return True
    except IOError:
        return False


# todo Подать изображение на распознавание текста
def get_serial_numbers(image_path: Path) -> List[str]:
    numbers = []
    text_ru = pytesseract.image_to_string(Image.open(image_path), 'rus')
    
    for idx, line in enumerate(text_ru.split('\n')):
        pattern = re.compile(r'заводской.*номер')
        if re.match(pattern, line):
            text_en = pytesseract.image_to_string(Image.open(image_path), 'eng')
            numbers.append(text_en.split('\n')[idx].split()[-1])
    return numbers


if __name__ == '__main__':
    pdf_path = Path('/Users/gefest/projects/geekbrains/data_mining_18_11_20/8416_4.pdf')
    images = pdf_image_extract(pdf_path)
    serials = {file.name: get_serial_numbers(file) for file in images}
    print(1)
