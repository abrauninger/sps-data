import camelot
import glob
import numpy
import os
import pandas
import pdfquery
import re
import time
import traceback

def extract_data_from_pdf(pdf_path: str) -> pandas.DataFrame:
	dataframes = []

	print(f"Loading PDF: '{pdf_path}'")
	pdf = pdfquery.PDFQuery(pdf_path)
	pdf.load()

	print(f"Processing PDF")
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

		if not numpy.allclose(individual_sums, total_sum):
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

	if not numpy.allclose(individual_sums, total_sum):
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

	return concatenated_df


def main():
	try:
		for pdf_path in glob.glob('./input/*.pdf'):
			filename = os.path.basename(pdf_path)

			m = re.match(r'^P223_(\D+)(\d+)\.pdf$', filename)
			if m is None:
				print(f"Unable to determine month and year from filename: '{filename}'")
				continue

			month_name_abbreviated = m.group(1)
			year_two_digit = m.group(2)

			parsed_month = time.strptime(f'{month_name_abbreviated} {year_two_digit}', '%b %y')
			month_name = time.strftime('%B %Y', parsed_month)

			df = extract_data_from_pdf(pdf_path)

			df['Month'] = month_name

			# Move 'Month' to the beginning of the column list
			columns = df.columns.tolist()
			columns = ['Month'] + [column for column in columns if column != 'Month']
			df = df[columns]

			print(df)

		#return df

	except Exception as e:
		traceback.print_exc()
		print('')

		import pdb
		pdb.post_mortem()


if __name__ == "__main__":
    main()
