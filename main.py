import camelot
import glob
import multiprocessing
import numpy
import os
import pandas
import pathlib
import pdfquery
import re
import time
import traceback

from typing import Callable, List, NamedTuple

def extract_data_from_pdf(pdf_path: str, month: str, output_csv_path: str, on_pdf_load: Callable):
	dataframes = []

	#print(f"Loading PDF: '{pdf_path}'")
	pdf = pdfquery.PDFQuery(pdf_path)
	pdf.load()
	on_pdf_load()

	#print(f"Processing PDF: '{pdf_path}'")
	pages = pdf.pq('LTPage')

	numeric_columns = [
		'Regular Program',
		'Bilingual Served',
		'Spec. Ed. Served',
		'Male',
		'Female',
		'Non-Binary',
		'Total Student Count',
		'P223 Total Count',
		'P223 Total FTE'
	]

	# Tables start on page index 1 (page id 2)
	for page_index in range(1, len(pages)):
		page_id = page_index + 1

		pages = pdf.pq(f'LTPage[pageid="{page_id}"]')

		if len(pages) != 1:
			print(f"Could not find a page with ID {page_id}")
			continue

		page_left = 0
		page_right = float(pages[0].attrib['x1'])

		text_boxes = pdf.pq(f'LTPage[pageid="{page_id}"] LTTextBoxHorizontal:contains("School")')

		school_label = None

		for text_box in text_boxes:
			m = re.match(r"^School\:(.*)$", text_box.text.strip())
			if m:
				school_label = text_box
				break

		if school_label is None:
			print(f"Could not find 'School:' text box on page ID {page_id}")
			continue

		school_label_top = school_label.attrib['y0']
		school_label_bottom = school_label.attrib['y1']

		extracted = pdf.extract([
			('with_parent', f'LTPage[pageid="{page_id}"]'),
			('school_name', f'LTTextBoxHorizontal:overlaps_bbox("{page_left},{school_label_top},{page_right},{school_label_bottom}")')
		])

		school_name_textboxes = extracted['school_name']
		if len(school_name_textboxes) == 0:
			print(f"Could not find the school name on page ID {page_id}")
			continue

		school_name = None

		for school_name_textbox in school_name_textboxes:
			m = re.match(r"^School\:(.+)$", text_box.text.strip())
			if m:
				school_name = m.group(1).strip()
				break

		if school_name is None:
			school_name = school_name_textboxes[-1].text.strip()

		if school_name is None:
			print(f"WARNING: Couldn't find the school name on page ID {page_id}")

		# For some reason the beginning N in NOTES is truncated
		text_boxes = pdf.pq(f'LTPage[pageid="{page_id}"] LTTextBoxHorizontal:contains("OTES:")')
		if len(text_boxes) != 1:
			print(f"Could note find 'NOTES:' text box on page ID {page_id}")
			continue

		notes_line = text_boxes[0]

		table_top = float(school_label.attrib['y0'])
		table_bottom = float(notes_line.attrib['y1'])

		tables = camelot.read_pdf(pdf_path, pages=f'{page_id}', flavor='stream', split_text=True, table_areas=[f'{page_left},{table_top},{page_right},{table_bottom}'])
		if len(tables) == 0:
			print(f"Could not read table from page ID {page_id}")
			continue

		if len(tables) > 1:
			print(f"Found more than one table on page ID {page_id}")
			continue

		df = tables[0].df

		# Read the same table without 'split_text' to get more stable column headers
		columns_df = camelot.read_pdf(pdf_path, pages=f'{page_id}', flavor='stream', table_areas=[f'{page_left},{table_top},{page_right},{table_bottom}'])[0].df

		first_data_row_index = 0
		while df.iat[first_data_row_index, 1] == '':
			first_data_row_index = first_data_row_index + 1

		first_data_row_index_in_columns_df = 0
		while columns_df.iat[first_data_row_index_in_columns_df, 1] == '':
			first_data_row_index_in_columns_df = first_data_row_index_in_columns_df + 1

		# Set the first two column headers
		# (We'll drop the UNUSED column later)
		columns_df.iat[0,0] = "UNUSED"
		columns_df.iat[0,1] = "Grade"

		last_column_header_row_index = first_data_row_index_in_columns_df - 1

		# Merge the rows of column headers
		df.loc[first_data_row_index-0.5] = columns_df.loc[0:last_column_header_row_index].agg(" ".join).apply(lambda s: s.strip())
		df = df.sort_index().reset_index(drop=True)

		# Drop the original rows of (unmerged) column headers
		df = df.loc[first_data_row_index:]

		# Use the first row as column headers
		df.columns = df.iloc[0]
		df = df[1:]

		# Clean up the grade values that get weirdly merged together

		# Elementary schools
		df.loc[df['Grade'] == '5 6 7', 'Grade'] = 5

		# Middle schools
		df.loc[df['Grade'] == '3 4 5 6', 'Grade'] = 6
		df.loc[df['Grade'] == '8 9', 'Grade'] = 8
		df.loc[df['Grade'] == '1\n0', 'Grade'] = 10

		# High schools
		df.loc[df['Grade'] == '5 6 7 8 9', 'Grade'] = 9
		df.loc[df['Grade'] == '1\n0', 'Grade'] = 10

		df = df.drop('UNUSED', axis=1)

		# Convert numeric columns
		for numeric_column in numeric_columns:
			df[numeric_column] = pandas.to_numeric(df[numeric_column])

		df = df[df['Total Student Count'].notnull()]

		# Check that totals add up properly
		individual_sums = df[df['Grade'] != 'Total'][numeric_columns].sum()
		total_sum = df[df['Grade'] == 'Total'][numeric_columns].sum()

		if not numpy.allclose(individual_sums, total_sum, atol=0.1):
			print(f"WARNING: Numbers don't seem to add up for '{school_name}'.")
			print("")
			print("Sum of individual grade levels:")
			print(individual_sums.to_string())
			print("")
			print("Total as reported:")
			print(total_sum.to_string())
			print("")
			print("")

		df = df[df['Grade'] != 'Total']

		df['School'] = school_name

		columns_in_desired_order = [
			'School',
			'Grade',
			'Regular Program',
			'Bilingual Served',
			'Spec. Ed. Served',
			'Male',
			'Female',
			'Non-Binary',
			'Total Student Count',
			'P223 Total Count',
			'P223 Total FTE'
		]

		df = df[columns_in_desired_order]

		dataframes.append(df)

	concatenated_df = pandas.concat(dataframes)

	# Check that the district totals add up properly
	individual_sums = concatenated_df[concatenated_df['School'] != 'District Total'][numeric_columns].sum()
	total_sum = concatenated_df[concatenated_df['School'] == 'District Total'][numeric_columns].sum()

	if not numpy.allclose(individual_sums, total_sum, atol=0.1):
		print(f"WARNING: District-wide numbers don't seem to add up for '{pdf_path}'.")
		print("")
		print("Sum of schools:")
		print(individual_sums.to_string())
		print("")
		print("District totals:")
		print(total_sum.to_string())
		print("")
		print("")

	# Drop 'District Total' from the concatenated data
	concatenated_df = concatenated_df[concatenated_df['School'] != 'District Total']

	concatenated_df['Month'] = month

	# Move 'Month' to the beginning of the column list
	columns = concatenated_df.columns.tolist()
	columns = ['Month'] + [column for column in columns if column != 'Month']
	concatenated_df = concatenated_df[columns]

	# Sort by school name
	concatenated_df = concatenated_df.sort_values(['School'])

	output_directory = os.path.dirname(output_csv_path)
	pathlib.Path(output_directory).mkdir(parents=True, exist_ok=True)

	concatenated_df.to_csv(output_csv_path)

	#print(f"Data from PDF '{pdf_path}' written to '{output_csv_path}'")


def month_from_pdf_file_name(pdf_path) -> str:
	filename = os.path.basename(pdf_path)

	m = re.match(r'^P223_(\D+)(\d+)\.pdf$', filename)
	if m is None:
		raise Exception(f"Unable to determine month and year from filename: '{filename}'")

	month_name_abbreviated = m.group(1)
	year_two_digit = m.group(2)

	parsed_month = time.strptime(f'{month_name_abbreviated} {year_two_digit}', '%b %y')
	month = time.strftime('%Y-%m', parsed_month)

	return month


class ExtractTaskInputs(NamedTuple):
	pdf_path: str
	month: str
	output_csv_path: str


def get_task_inputs(pdf_path: str, output_directory: str) -> ExtractTaskInputs:
	month = month_from_pdf_file_name(pdf_path)
	output_csv_path = f'{output_directory}/{month}.csv'

	return ExtractTaskInputs(pdf_path, month, output_csv_path)


def extract_worker(task_queue, done_queue):
	try:
		on_pdf_load = lambda: done_queue.put(['loaded_pdf', inputs.pdf_path])

		for inputs in iter(task_queue.get, 'STOP'):
			extract_data_from_pdf(inputs.pdf_path, inputs.month, inputs.output_csv_path, on_pdf_load)
			done_queue.put(['finished_pdf', inputs.pdf_path, inputs.output_csv_path])
	except Exception as e:
		traceback.print_exc()
		print('')

		# import pdb
		# pdb.post_mortem()


class Progress:
	completed_tasks: int
	total_tasks: int

	def __init__(self, completed_tasks: int, total_tasks: int):
		self.completed_tasks = completed_tasks
		self.total_tasks = total_tasks

	def report(self, message: str, increment_completed=True):
		if increment_completed:
			self.completed_tasks = self.completed_tasks + 1

		print(f"[{self.completed_tasks}/{self.total_tasks}] {message}")


def extract_all_pdfs(input_directory: str, output_directory: str) -> List[str]:
	pdf_paths = glob.glob(f'{input_directory}/*.pdf')

	tasks = [get_task_inputs(pdf_path, output_directory) for pdf_path in pdf_paths]

	tasks.sort(key=lambda task: task.month)

	task_queue = multiprocessing.Queue()
	done_queue = multiprocessing.Queue()

	for task in tasks:
		task_queue.put(task)

	PROCESS_COUNT = 8
	for _ in range(PROCESS_COUNT):
		multiprocessing.Process(target=extract_worker, args=(task_queue, done_queue)).start()

	output_csv_paths: List[str] = []

	completed_progress = 0

	# Each PDF task has two big chunks of work: Load PDF, and extract data from it
	total_expected_done_queue_count = len(tasks) * 2

	progress = Progress(completed_tasks=0, total_tasks=total_expected_done_queue_count+1)

	progress.report(f"Extracting {len(pdf_paths)} PDF(s)", increment_completed=False)

	for _ in range(total_expected_done_queue_count):
		done_queue_item = done_queue.get()
		match done_queue_item:
			case ["loaded_pdf", pdf_path]:
				progress.report(f"Loaded PDF: {pdf_path}")
			case ["finished_pdf", pdf_path, output_csv_path]:
				output_csv_paths.append(output_csv_path)
				progress.report(f"Extracted data from {pdf_path} to {output_csv_path}")

	# Shut down child processes
	for _ in range(PROCESS_COUNT):
		task_queue.put('STOP')

	return (output_csv_paths, progress.total_tasks)


def main():
	month_csv_files, progress_total_tasks = extract_all_pdfs('input', 'output/p223/month')

	dataframes = [pandas.read_csv(month_csv_file) for month_csv_file in month_csv_files]

	concatenated_df = pandas.concat(dataframes)
	
	concatenated_df = concatenated_df.sort_values(['Month', 'School'])

	output_csv_path = 'output/p223/all.csv'
	pathlib.Path(os.path.dirname(output_csv_path)).mkdir(parents=True, exist_ok=True)
	concatenated_df.to_csv(output_csv_path)

	Progress(completed_tasks=progress_total_tasks, total_tasks=progress_total_tasks, increment_completed=False).report(f"Data from all PDFs written to '{output_csv_path}'")


if __name__ == "__main__":
    main()
