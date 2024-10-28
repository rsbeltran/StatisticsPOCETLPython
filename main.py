from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
from fpdf import FPDF

servername="LAPTOP-LQPL001R\SQLEXPRESS"
dbname="POC_PYTHON_ETL"
 
DATABASE_URL = 'mssql+pyodbc://' + servername + '/' + dbname + '?trusted_connection=yes&driver=SQL+Server+Native+Client+11.0'
engine = create_engine(DATABASE_URL)

# Consultar tabla ResponseMoviesAPI
connection=engine.connect()

query = "SELECT*  FROM [POC_PYTHON_ETL].[dbo].[ResponseMoviesAPI]"  
dfResponse = pd.read_sql(query, connection)
print(dfResponse.head())

dfResponse['Runtime']=pd.to_numeric(dfResponse['Runtime'], errors='coerce')

# Consultar tabla Input MoviesInicial
query = "SELECT*  FROM [POC_PYTHON_ETL].[dbo].[MoviesInicial]"  
dfInput = pd.read_sql(query, connection)

#Cerrar conexión
connection.close
dfInput['APIResponse']=dfInput['Title'].isin(dfResponse['Title'])

#Función para generar tabla de PDF
def output_df_to_pdf(pdf,df):
    table_cell_width=25
    table_cell_height=6
    pdf.set_font('Arial','B',10)
    cols=df.columns
    for col in cols:
        pdf.cell(120 if col == 'Title' else 25,table_cell_height,col,align='C',border=1)
    pdf.ln(table_cell_height)
    pdf.set_font('Arial','',8)
    for row in df.itertuples():
        for col in cols:
            value=str(getattr(row,col))
            pdf.cell(120 if col == 'Title' else 25,table_cell_height,value,align='C',border=1)
        pdf.ln(table_cell_height)



#Filtrar dataframe con películas no encontradas
dfFilteredv1=dfInput[dfInput['APIResponse']==False]
dfFilteredv2=dfFilteredv1[['Title','Year']]

#Estadísticas
total_movies = len(dfResponse)
total_movies_Input=len(dfInput)
movies_per_year = dfResponse['Year'].value_counts()
average_Runtime= dfResponse['Runtime'].mean().round(2)

# Generar reporte pdf
pdf = FPDF()
pdf.add_page()
pdf.set_font('Arial','B',size=20)
page_width = pdf.w - 2 * pdf.l_margin
pdf.cell(page_width, 10, 'Processing Report', align='C')
pdf.ln(20)
pdf.set_font('Arial','B',size=15)
pdf.cell(40,10,'General Statistics')
pdf.ln(20)

pdf.cell(0, 10, f"Total movies with API response: {total_movies}.", ln=True)
pdf.cell(0, 10, f"Total movies without API response: {total_movies}.", ln=True)
pdf.cell(0, 10, f"Total movies per year: {movies_per_year}.", ln=True)
pdf.cell(0, 10, f"Average Runtime: {average_Runtime}.", ln=True)

#Generar pdf
output_df_to_pdf(pdf,dfFilteredv2)
pdf.output('Processing_Report.pdf','F')






