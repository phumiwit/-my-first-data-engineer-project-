# My-first-data-engineer-project-Datapipeline-For-Simulate-data-cleaning-
โปรเจคนี้ เป็นการทดลองการทำความสะอาดข้อมูลโดยใช้ Dataproc ในการรัน sparkjob บน Google Cloud Platform ร่วมกับการสร้าง Datapipeline โดยใช้ Apache Airflow บน Composer ซึ่งเป็นบริการ Orchestration  
และใช้ในการดึงข้อมูลจาก storage ต่างๆมาประมวลผลตามลำดับ โดยเมื่อทำความสะอาดข้อมูลเสร็จแล้วจะนำข้อมูลนั้นๆไปใส่ใน google bigguery โดยสร้างตารางตามความต้องการของผู้ใช้งาน(สมมุติขึ้นมา)เพื่อใช้ในการวิเคราะห์ข้อมูลต่อไป
