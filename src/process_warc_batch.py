import string
import traceback
import time

from os import listdir
from os.path import isfile, join

from bs4 import BeautifulSoup
from selectolax.parser import HTMLParser
from warcio.archiveiterator import ArchiveIterator



digits = string.printable[0:10]
special_chars = string.printable[62:95]
keep_chars = ['"', '%', "'", '(', ')', ',', '-', '.', ':', ';', '/', '?', '@', '!', '\n', '\t', '\'']
malayalam_unicode_decimal_list = list(range(3328, 3456)) + [8204, 8205]


def is_char_malayalam(c):
    if ord(c) in malayalam_unicode_decimal_list:
        return True
    else:
        return False

# def is_char_malayalam(c):
#     if 3328 <= ord(c) <= 3455 or ord(c) == 8205 or ord(c) == 8204:
#         return True
#     else:
#         return False


def is_context_malayalam(past_chars):
    count = 0

    for char in past_chars:
        if is_char_malayalam(char):
            count += 1

    if count > 3:
        return True
    else:
        return False


def get_malayalam_text(html):
    out = []
    mal_block = False
    past_chars_queue = []

    for char in html:
        if len(past_chars_queue) < 3:
            past_chars_queue.append(char)
        else:
            if mal_block:
                if not is_context_malayalam(past_chars_queue):
                    mal_block = False
            else:
                if is_context_malayalam(past_chars_queue):
                    mal_block = True

        if len(past_chars_queue) == 10:
            past_chars_queue.pop(0)
        past_chars_queue.append(char)

        if is_char_malayalam(char) or ord(char) == 32:
            out.append(char)
        elif (char in digits or char in keep_chars) and mal_block:
            out.append(char)

    out = "".join(out)
    return out


def get_text_bs(html):
    tree = BeautifulSoup(html, 'lxml')

    tree_body = tree.body
    if tree_body is None:
        return None

    for tag in tree_body.select('script'):
        tag.decompose()
    for tag in tree_body.select('style'):
        tag.decompose()

    parsed_text = tree_body.get_text(separator='\n')
    return parsed_text


def get_olax_header_and_paragraphs(html):
    html_parser = HTMLParser(html)
    parsed_text = ""
    selector = "h1"
    for node in html_parser.body.css(selector):
        parsed_text = parsed_text + node.text(deep=True, separator='', strip=True)
        parsed_text = parsed_text + "\n"

    selector = "p"
    for node in html_parser.body.css(selector):
        parsed_text = parsed_text + node.text(deep=True, separator='', strip=True)
        parsed_text = parsed_text + "\n"
    return parsed_text


def get_text_selectolax(html):
    html_parser = HTMLParser(html)

    if html_parser.body is None:
        return None

    for tag in html_parser.css('script'):
        tag.decompose()
    for tag in html_parser.css('style'):
        tag.decompose()

    parsed_text = html_parser.body.text(separator='\n')
    return parsed_text

FILTERED_DIRECTORY = "malayalam_filtered_html_body/"
UNFILTERED_DIRECTORY = "unfiltered_heading_and_para/"

if __name__ == '__main__':
    INPUT_DIRECTORY_PATH = "warcs/"
    files = [join(INPUT_DIRECTORY_PATH, f) for f in listdir(INPUT_DIRECTORY_PATH) if
             isfile(join(INPUT_DIRECTORY_PATH, f))]

    OUTPUT_DIRECTORY_PATH = "out/"

    for file in files:
        start = time.time()
        with open(file, 'rb') as stream:
            records_count = 0
            exceptions_count = 0

            out_name_base = str(str(file.split('.')[0]).split("-")[-1])

            out_name_heading_para = UNFILTERED_DIRECTORY + out_name_base + "_heading_para" + ".txt"
            out_name_html_body = FILTERED_DIRECTORY + out_name_base + "_html_body" + ".txt"

            print(f"Output File Prefix  :{out_name_base}")

            for record in ArchiveIterator(stream):
                if record.rec_type == 'response':
                    records_count += 1
                    print(records_count)

                    try:
                        body = record.content_stream().read().decode("utf-8", "replace")
                        uri = record.rec_headers.get_header('WARC-Target-URI')

                        extracted_heading_para = get_olax_header_and_paragraphs(body)
                        # extracted_heading_para = get_malayalam_text(extracted_heading_para)
                        if len(extracted_heading_para) > 20:
                            with open(out_name_heading_para, "a") as f:
                                f.write(extracted_heading_para)
                                f.write("\n")

                        html_visible_body = get_text_selectolax(body).strip()
                        html_lines = html_visible_body.split("\n")
                        html_lines_filtered = []
                        for line in html_lines:
                            stripped = line.strip()
                            if len(stripped) > 30:
                                html_lines_filtered.append(stripped)

                        if html_lines_filtered:
                            text = "\n".join(html_lines_filtered)
                            extracted_malayalam = ""
                            for line in html_lines_filtered:
                                malayalam_line = get_malayalam_text(line).strip()
                                if len(malayalam_line) > 2:
                                    extracted_malayalam = extracted_malayalam + malayalam_line + "\n"

                            with open(out_name_html_body, "a") as f:
                                f.write(extracted_malayalam)
                                f.write("\n")

                    except Exception as e:
                        exceptions_count += 1
                        # print(e)
                        traceback.print_exc()
            end = time.time()
            time_taken = (end - start) / 60
            print(f"Time taken for processing file {file}: {time_taken} minutes")
            print(f"No of exceptions = {exceptions_count}")
            print(f"No of records processed in file: {file}  is {records_count}")

