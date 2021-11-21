# Databricks notebook source
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import smtplib
from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from PIL import Image

# COMMAND ----------

storage_key = "B8EKaguhL0X72HMh1QrHRv7jIHHgAdhxcAI1FY8SDswZ3F26WRWJBhX5iyT4QCC+8Xz4vNdMyHM0NlTvsqWzkg=="
container_path = "abfss://astrosabana@dpericostorage.dfs.core.windows.net"
spark.conf.set("fs.azure.account.key.dpericostorage.dfs.core.windows.net", storage_key)
spark.conf.set("spark.sql.session.timeZone", "America/Bogota")

# COMMAND ----------

today_obj = datetime.today() - timedelta(hours = 5)
today_day = today_obj.day
today_month = today_obj.month

# COMMAND ----------

months_map_dict = {1 : "Enero", 2 : "Febrero", 3 : "Marzo", 4 : "Abril", 5 : "Mayo", 6 : "Junio", 7 : "Julio", 8 :"Agosto",9 : "Septiembre", 10 : "Octubre", 11 : "Noviembre", 12 : "Diciembre"}

# COMMAND ----------

def translate_month_number_spanish(month_number: int) -> str:
    return months_map_dict[month_number]

# COMMAND ----------

month_str_spanish = translate_month_number_spanish(today_month)
calendar_df = spark.read.format("csv").option("header", "true").load(container_path + '/AstroCalendar.csv')
contacts_df = spark.read.option("multiline", "true").json(container_path + '/Contacts.json')

# COMMAND ----------

exploded_contacts_df = contacts_df.withColumn("exploded_row", F.explode("contacts_info")).withColumn("Name", F.col("exploded_row").Name).withColumn("Surname", F.col("exploded_row").Surname).withColumn("Email", F.col("exploded_row").Email).drop("contacts_info","exploded_row")
contacts_info_list = exploded_contacts_df.select("Name","Email").collect()

# COMMAND ----------

filtered_calendar_df = calendar_df.where(f"Month == '{month_str_spanish}' and Day == '{today_day}'").withColumn("Event", F.initcap(F.col("Event"))).orderBy("Year")
filtered_calendar_pdf = filtered_calendar_df.toPandas()
filtered_calendar_html = filtered_calendar_pdf.to_html(index = False)

# COMMAND ----------

from_addr = "iusepythonbtw@gmail.com"
password = dbutils.secrets.get("AstroScope", "PythonMailPass")
smtp='smtp.gmail.com'

# COMMAND ----------

foo = Image.open("/dbfs/FileStore/images/AstroSabana_Logo_Blanco.jpg")
foo = foo.resize((500,220),Image.ANTIALIAS)
foo.save("/dbfs/FileStore/images/AstroSabana_Logo_Blanco_Small.jpg",optimize = True, quality = 95)

# COMMAND ----------

for name, email in contacts_info_list: 
    
    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = 'AstroSabana Events Calendar'
    msgRoot['From'] = from_addr
    msgRoot['To'] = email
    msgRoot.preamble = 'This is a multi-part message in MIME format.'
     
    # Encapsulate the plain and HTML versions of the message body in an
    # 'alternative' part, so message agents can decide which they want to display.
    msgAlternative = MIMEMultipart('alternative')
    msgRoot.attach(msgAlternative)
     
    # We reference the image in the IMG SRC attribute by the ID we give it below
    html=(f'''<html><body><h1>AstroCalendar</h1><p>¡Hola {name}!.Estos son los eventos astronómicos más relevantes en la historia para la fecha de hoy:</p><br>{filtered_calendar_html}<br><img src="cid:image1"><br></body></html>''')
    msgText = MIMEText(html, 'html')
    msgAlternative.attach(msgText)
     
    # This example assumes the image is in the current directory
    fp = open("/dbfs/FileStore/images/AstroSabana_Logo_Blanco_Small.jpg", 'rb')
    msgImage = MIMEImage(fp.read())
    fp.close()
     
    # Define the image's ID as referenced above
    msgImage.add_header('Content-ID', '<image1>')
    msgRoot.attach(msgImage)
  
    #Send Email
    s = smtplib.SMTP(smtp, 587) 
    s.starttls() 
    s.login(from_addr, password) 
    s.sendmail(from_addr, email, msgRoot.as_string()) 
    s.quit() 