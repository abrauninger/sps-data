import camelot
import numpy
import pandas
import pdfquery
import re
import traceback  # TODO: Remove

def main():
	try:
		dataframes = []

		pdf_path = '/Users/andy/repos/sps-data/input/P223_Sep24.pdf'

		print(f"Loading PDF: '{pdf_path}'")
		pdf = pdfquery.PDFQuery(pdf_path)
		pdf.load()

		print(f"Processing PDF")
		pages = pdf.pq('LTPage')

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

			df = tables[0].df

			# Set the first two column headers
			# (We'll drop the UNUSED column later)
			df.iat[0,0] = "UNUSED"
			df.iat[0,1] = "Grade"

			# Merge the first two rows of column headers
			df.loc[1.5] = df.loc[0:1].agg(" ".join).apply(lambda s: s.strip())
			df = df.sort_index().reset_index(drop=True)

			# Drop the original first two rows
			df = df.loc[2:]

			# Use the first row as column headers
			df.columns = df.iloc[0]
			df = df[1:]

			# Clean up some column names
			if 'To on-Binary' in df.columns:
				df = df.rename({'To on-Binary': 'Total Non-Binary', 'tal Student Count': 'Total Student Count', 'Female\nN': 'Female'}, axis=1)
			elif 'To n-Binary' in df.columns:
				df = df.rename({'To n-Binary': 'Total Non-Binary', 'tal Student Count': 'Total Student Count', 'Female\nNo': 'Female'}, axis=1)
			elif 'Tot n-Binary' in df.columns:
				df = df.rename({'Tot n-Binary': 'Total Non-Binary', 'al Student \nP Count': 'Total Student Count', 'Female\nNo': 'Female', '223 Total \nP Count': 'P223 Total Count', '223 Total Count': 'P223 Total Count', '223 Total FTE': 'P223 Total FTE'}, axis=1)
			elif 'To Non-Binary' in df.columns:
				df = df.rename({'To Non-Binary': 'Total Non-Binary', 'tal Student Count': 'Total Student Count'}, axis=1)
			elif 'Tot Non-Binary' in df.columns:
				df = df.rename({'Tot Non-Binary': 'Total Non-Binary', 'al Student Count': 'Total Student Count'}, axis=1)

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
			numeric_columns = [
				'Bilingual Served',
				'Spec. Ed. Served',
				'Male',
				'Female',
				'Total Non-Binary',
				'Total Student Count',
				'P223 Total Count',
				'P223 Total FTE'
			]

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
			dataframes.append(df)

		large_df = pandas.concat(dataframes)

		print(large_df)

	except Exception as e:
		traceback.print_exc()
		print('')

		import pdb
		pdb.post_mortem()

if __name__ == "__main__":
    main()
