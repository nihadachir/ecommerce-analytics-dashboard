# 🛒 E-commerce Sales Analytics Dashboard

## 📌 Project Overview
This project analyzes e-commerce sales data to provide business insights through an interactive **Power BI dashboard**.  
The goal is to help stakeholders track sales performance, understand customer behavior, and identify product trends.  

## 🛠️ Tools & Technologies
- **Python (Pandas)** → data cleaning & preprocessing  
- **SQL (PostgreSQL)** → data modeling (fact & dimension tables), analytical queries  
- **Power BI** → dashboard creation & visualization  
- *(Optional)*: Python (scikit-learn) → churn prediction model  

## 📂 Project Structure
├── data/
│ ├── raw/ # Raw dataset (from Kaggle)
│ ├── clean/ # Cleaned dataset after preprocessing
├── notebooks/ # Jupyter notebooks for data cleaning & exploration
├── sql/ # DDL & analytical queries (fact/dim tables, RFM analysis, KPIs)
├── src/ # Python scripts for ETL & preprocessing
├── powerbi/ # Power BI dashboard file (.pbix)
└── README.md # Project documentation


## 🔑 Key Features
- **Data Cleaning (Python)**: handled missing values, removed duplicates, formatted dates & text  
- **SQL Analysis**:
  - Revenue per month & year-over-year comparison  
  - Top products & most valuable customers  

- **Power BI Dashboard**:
  - *Sales Overview*: KPIs, monthly trends, revenue by country  
  - *Customer Insights*: RFM clusters, new vs returning customers  
  - *Product Insights*: top products, units vs revenue, low-performing items  
  

## 📊 Dashboard Preview
*(Insert screenshot of your dashboard here once ready)*

## 🚀 How to Run
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/ecommerce-analytics-dashboard.git
   cd ecommerce-analytics-dashboard
