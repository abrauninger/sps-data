import camelot
import pdfquery
import re

pdf_path = '/Users/andy/repos/sps-data/input/P223_Sep24.pdf'
pdf = pdfquery.PDFQuery(pdf_path)
pdf.load()

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

	top_element = None
	school_name: str | None = None

	for text_box in text_boxes:
		m = re.match(r"^School\:(.*)$", text_box.text.strip())
		if m:
			top_element = text_box
			school_name = m.group(1).strip()
			break

	if top_element is None:
		print(f"Could not find 'School:' text box on page ID {page_id}")
		continue

	# For some reason the beginning N in NOTES is truncated
	text_boxes = pdf.pq(f'LTPage[pageid="{page_id}"] LTTextBoxHorizontal:contains("OTES:")')
	if len(text_boxes) != 1:
		print(f"Could note find 'NOTES:' text box on page ID {page_id}")
		continue

	bottom_element = text_boxes[0]

	table_top = float(top_element.attrib['y0'])
	table_bottom = float(bottom_element.attrib['y1'])

	tables = camelot.read_pdf(pdf_path, pages=f'{page_id}', flavor='stream', table_areas=[f'{page_left},{table_top},{page_right},{table_bottom}'])

	df = tables[0].df

	breakpoint()

tables = camelot.read_pdf(pdf_path)