import csv
import sys


def remove_duplicates(new_csv, old_csv):
    new_urls = []
    old_urls = []
    with open(new_csv, 'r') as file:
        csv_reader = csv.reader(file)
        for csv_row in csv_reader:
            new_urls.append(csv_row[0])

    print(f"No of urls in new csv :{len(new_urls)}")

    with open(old_csv, 'r') as file:
        csv_reader = csv.reader(file)
        for csv_row in csv_reader:
            old_urls.append(csv_row[0])

    print(f"No of urls in existing csv :{len(old_urls)}")

    f1_urls_set = set(new_urls)
    duplicates = set(f1_urls_set.intersection(old_urls))
    print(f"No of duplicate urls in new csv :{len(duplicates)}")
    out_rows = []
    with open(new_csv, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if len(duplicates.intersection({row[0]})) == 0:
                out_rows.append(row)
    return out_rows


if __name__ == '__main__':
    args = sys.argv[1:]
    if len(args) == 3:
        incoming_file = args[0]
        old_file = args[1]
        out_file = args[2]
    else:
        print("Wrong number of arguments")
        exit()

    filtered_rows = remove_duplicates(incoming_file, old_file)
    print(f"No of new urls :{len(filtered_rows)}")

    with open(out_file, "w") as file:
        fieldnames = ["url", "warc_filename", "warc_record_offset", "warc_record_length"]
        # fieldnames = ['first_name', 'last_name']
        writer = csv.DictWriter(file, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for entry in filtered_rows:
            writer.writerow({'url': entry[0], 'warc_filename': entry[1], 'warc_record_offset': entry[2],
                             'warc_record_length': entry[3]})
