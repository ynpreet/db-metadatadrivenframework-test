# SQL Server to Databricks Migration Project

![Project Banner](https://via.placeholder.com/1200x300?text=SQL+Server+to+Databricks+Migration)

## 🚀 Overview
This project focuses on migrating data from Microsoft SQL Server to Databricks using PySpark. The goal is to enable scalable, efficient, and robust data pipelines for data analytics and machine learning use cases.

---

## 📋 Objectives

1. **Migrate Data**: Seamlessly transfer data from SQL Server to Databricks.
2. **Optimize Pipelines**: Implement PySpark transformations for performance.
3. **Ensure Security**: Secure the data during transit and storage.
4. **Leverage Delta Lake**: Utilize Databricks Delta Lake for ACID transactions.

---

## 🛠️ Technologies Used

- **Databricks**: For scalable and collaborative data engineering.
- **Microsoft SQL Server**: Source database for structured data.
- **PySpark**: For distributed data processing.
- **Azure Data Lake Gen2**: For storing processed data.
- **Delta Lake**: For managing data versioning and ACID compliance.

![Tech Stack](https://via.placeholder.com/800x200?text=Databricks+%7C+PySpark+%7C+SQL+Server+%7C+Delta+Lake)

---

## 🏗️ Project Architecture

1. **Extract**: Pull data from SQL Server using JDBC.
2. **Transform**: Use PySpark for cleaning, enriching, and optimizing data.
3. **Load**: Write data to Databricks in Delta Lake format.

![Architecture Diagram](https://via.placeholder.com/800x400?text=SQL+Server+%E2%86%92+Databricks+Pipeline+Architecture)

---

## 📝 Key Features

- **Incremental Loading**: Only migrate new or updated records.
- **Schema Validation**: Ensure consistency between SQL Server and Databricks.
- **Error Handling**: Robust logging and retry mechanisms.
- **Data Quality Checks**: Validate the integrity of data during migration.

---

## 📂 Folder Structure

```
project/
├── notebooks/               # Databricks notebooks
├── scripts/                 # PySpark scripts
├── config/                  # Configuration files (JDBC, secrets)
├── logs/                    # Logs for debugging
└── README.md                # Project documentation
```

---

## 🖼️ Sample Screenshots

### Databricks Pipeline:
![Databricks Pipeline Screenshot](https://via.placeholder.com/800x400?text=Databricks+Pipeline+in+Action)

### SQL Server Query Editor:
![SQL Server Query Editor Screenshot](https://via.placeholder.com/800x400?text=SQL+Server+Query+Editor)

---

## 💡 How to Run

1. Clone this repository.
2. Set up the JDBC connection in `config/jdbc_config.json`.
3. Upload notebooks to Databricks.
4. Execute the notebooks in sequence.

---

## 🌟 Cool Highlights

- **PySpark Magic**: Transform millions of records in minutes.
- **Delta Lake Brilliance**: Enjoy ACID compliance for your data.
- **Visualization Power**: Integrate Databricks visualizations for insights.

![Cool Visualization](https://via.placeholder.com/800x400?text=Data+Visualizations+in+Databricks)

---

## 🙌 Contributors

- **[Preet Mehta](https://github.com/ynpreet)**: Data Engineer

---

## 📄 License
This project is licensed under the MIT License.

---

## 📫 Contact
If you have any questions or suggestions, feel free to reach out:

- Email: preetmehta1995@gmail.com
- LinkedIn: [Your LinkedIn Profile](https://www.linkedin.com/in/preetmehta/)

![Thank You](https://via.placeholder.com/1200x300?text=Thank+You+for+Exploring+the+Project)

