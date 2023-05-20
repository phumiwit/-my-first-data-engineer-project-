# My-first-data-engineer-project-Datapipeline-For-Simulate-data-cleaning-
โปรเจคนี้ เป็นการทดลองการทำความสะอาดข้อมูลโดยใช้ Dataproc ในการรัน sparkjob บน Google Cloud Platform ร่วมกับการสร้าง Data pipeline โดยใช้ Apache Airflow บน Composer ซึ่งเป็นบริการ Orchestration  
และใช้ในการดึงข้อมูลจาก storage ต่างๆมาประมวลผลตามลำดับ โดยเมื่อทำความสะอาดข้อมูลเสร็จแล้วจะนำข้อมูลนั้นๆไปใส่ใน Google bigguery โดยสร้างตารางตามความต้องการของผู้ใช้งาน(สมมุติขึ้นมา)เพื่อใช้ในการวิเคราะห์ข้อมูลต่อไป

# Datapipeline
<img src = 'images/retail data (1).jpg'>


# Dataset ที่ใช้
ผมได้ใช้ Dataset จาก 
https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/retail-data/all/online-retail-dataset.csv 
ซึ่งเป็นข้อมูล Online retail เนื่องจากมีข้อมูลให้ทำความสะอาดข้อมูลหลากหลายรูปแบบ ในการทำความสะอาดข้อมูลนั้นผมได้สมมุติ requirements ต่างๆขึ้นมาในการทำความสะอาดข้อมูลตาม requirement นั้นๆ
ซึ่งขึ้นตอนการทำความสะอาดข้อมูลสามารถดูได้ที่ ... และสามารถ Download ใน github ได้เลยครับ





