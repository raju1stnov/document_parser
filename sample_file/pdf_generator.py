from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Image
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
import random
import os

# Sample data for vendors
departments = ["Retail", "Manufacturing", "Pharmacy", "IT"]
vendor_names = ["Vendor A", "Vendor B", "Vendor C", "Vendor D", "Vendor E"]
contact_names = ["John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Davis"]
emails = ["vendor@example.com", "contact@example.com", "info@example.com"]
phone_numbers = ["123-456-7890", "987-654-3210", "555-123-4567"]
addresses = ["123 Main St", "456 Elm St", "789 Oak St", "321 Pine St", "654 Maple St"]
descriptions = [
    """Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore 
    et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip 
    ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu 
    fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt 
    mollit anim id est laborum.""",
    # More descriptions...
]

def generate_vendor_data(num_vendors):
    vendor_data = []
    for _ in range(num_vendors):
        vendor = [
            random.choice(departments),
            random.choice(vendor_names),
            random.choice(contact_names),
            random.choice(emails),
            random.choice(phone_numbers),
            random.choice(addresses),
            random.choice(descriptions)
        ]
        vendor_data.append(vendor)
    return vendor_data

def create_pdf(filename):
    doc = SimpleDocTemplate(filename, pagesize=letter, pageCompression=False)
    elements = []

    # Header
    elements.append(Paragraph("Vendor Contact Management for Healthcare Company", getSampleStyleSheet()["Heading1"]))

    # Generate multiple tables for redundancy
    for _ in range(4):  # Repeat the same data multiple times to increase file size
        vendor_data = generate_vendor_data(50000)
        header = ["Department", "Vendor Name", "Contact Person", "Email", "Phone", "Address", "Description"]
        all_data = [header] + vendor_data
        table = Table(all_data, colWidths=[inch] * len(header))
        table.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), '#CCCCCC'),
            ('TEXTCOLOR', (0,0), (-1,0), '#000000'),
            ('ALIGN', (0,0), (-1,-1), 'LEFT'),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE', (0,0), (-1,0), 10),
            ('BOTTOMPADDING', (0,0), (-1,0), 12),
            ('BACKGROUND', (0,1), (-1,-1), '#EEEEEE'),
            ('GRID', (0,0), (-1,-1), 1, '#000000'),
        ]))
        elements.append(table)

    # Add images to increase file size
    img_path = "rag_architecture_cloud_function.jpg"
    if os.path.exists(img_path):
        for _ in range(2000):
            img = Image(img_path, width=8*inch, height=6*inch)
            elements.append(img)

    doc.build(elements)

if __name__ == "__main__":
    create_pdf("vendor_contact_management.pdf")