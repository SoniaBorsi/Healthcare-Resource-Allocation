import mlflow
import mlflow.spark
import mlflow.pyfunc
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id
import pandas as pd
import logging

from utils import create_spark_session, read_from_postgresql

def preprocess_data():
    budget_state_per_capital = pd.read_excel("Downloads/aihw-93-Health-expenditure-Australia-2021-2022.xlsx", 
                                             sheet_name="Table 5", usecols="A:X", skiprows=2, nrows=12)
    budget_state_per_capital = budget_state_per_capital.drop(columns=["Unnamed: 2", "Unnamed: 3", "Unnamed: 5", "Unnamed: 6", "Unnamed: 8", "Unnamed: 9", "Unnamed: 11", "Unnamed: 12", "Unnamed: 14", "Unnamed: 15", "Unnamed: 17", "Unnamed: 18", "Unnamed: 20", "Unnamed: 21", "Unnamed: 23"])
    budget_state_per_capital.columns = ["Year", "NSW", "Vic", "Qld", "WA", "SA", "Tas", "NT", "NAT"]
    budget_state_per_capital['Year'] = range(2011, 2022)
    budget_state_per_capital = budget_state_per_capital[["Year", "NSW", "Vic", "Qld", "SA", "WA", "Tas", "NT", "NAT"]]
    
    return budget_state_per_capital

def fit_models(budget_state_per_capital_spark):
    models = {}
    for col_name in budget_state_per_capital_spark.columns[1:]:
        with mlflow.start_run(run_name=f"Model for {col_name}"):
            assembler = VectorAssembler(inputCols=[col_name], outputCol="features")
            training_data = assembler.transform(budget_state_per_capital_spark)
            lr = LinearRegression(featuresCol="features", labelCol=col_name)
            model = lr.fit(training_data)
            
            # Log parameters, metrics, and model
            mlflow.log_param("label", col_name)
            mlflow.log_metric("intercept", model.intercept)
            mlflow.log_metric("slope", model.coefficients[0])
            mlflow.spark.log_model(model, f"model_{col_name}")
            
            models[col_name] = model
    
    return models

def read_population_data():
    population_prediction = pd.read_excel("Downloads/Projected population, Australia.xlsx", usecols="A:D", skiprows=1)
    population_historic = pd.read_excel("Downloads/ABS_ERP_COMP_Q_1.0.0_10..Q.xlsx", usecols="B:D", skiprows=7, nrows=44, header=None)
    population_historic = population_historic.iloc[::4].reset_index(drop=True)
    population_historic.columns = ["Year", "Population"]
    population_historic['Year'] = range(2011, 2022)
    population_historic['Population'] = population_historic['Population'] * 1000
    
    return population_prediction, population_historic

def predict_budget(population_historic, population_prediction, models, budget_state_per_capital_spark):
    assembler = VectorAssembler(inputCols=["Population"], outputCol="features")
    training_data = assembler.transform(spark.createDataFrame(population_historic))
    lr = LinearRegression(featuresCol="features", labelCol="Population")
    
    with mlflow.start_run(run_name="Budget Model"):
        budget_model = lr.fit(training_data)
        
        # Log budget model
        mlflow.log_param("label", "Population")
        mlflow.log_metric("intercept", budget_model.intercept)
        mlflow.log_metric("slope", budget_model.coefficients[0])
        mlflow.spark.log_model(budget_model, "budget_model")
        
        intercept = budget_model.intercept
        slope = budget_model.coefficients[0]

    population_prediction_spark = spark.createDataFrame(population_prediction)
    predictions_high = population_prediction_spark.withColumn("budget_prediction", intercept + slope * col("High series"))
    predictions_medium = population_prediction_spark.withColumn("budget_prediction", intercept + slope * col("Medium series"))
    predictions_low = population_prediction_spark.withColumn("budget_prediction", intercept + slope * col("Low series"))

    surgeries_pred_state = {}
    for col_name in models.keys():
        intercept = models[col_name].intercept
        slope = models[col_name].coefficients[0]

        surgeries_pred_state[col_name] = {
            "high": predictions_high.withColumn(col_name, intercept + slope * col("budget_prediction")).select("Year", col_name).toPandas(),
            "medium": predictions_medium.withColumn(col_name, intercept + slope * col("budget_prediction")).select("Year", col_name).toPandas(),
            "low": predictions_low.withColumn(col_name, intercept + slope * col("budget_prediction")).select("Year", col_name).toPandas()
        }
    
    return surgeries_pred_state

def run_ml_pipeline(spark):
    try:
        # Preprocess data
        budget_state_per_capital = preprocess_data()
        budget_state_per_capital_spark = spark.createDataFrame(budget_state_per_capital)
        
        # Read data from PostgreSQL
        df = read_from_postgresql(spark, "values")
        values_national = df.filter(col("ReportingUnitCode").isin("NSW", "Vic", "Qld", "SA", "WA", "Tas", "NAT"))
        values_national = values_national.withColumn("id", monotonically_increasing_id())
        values_national = values_national.filter(col("id") < 84).drop("id")

        values_wide_pandas = values_national.toPandas().pivot(index="DataSetId", columns="ReportingUnitCode", values="Value").reset_index(drop=True)
        values_wide_pandas['Year'] = range(2011, 2022)
        values_wide_pandas = values_wide_pandas[["Year", "NSW", "Vic", "Qld", "SA", "WA", "Tas", "NAT"]]

        values_wide_spark = spark.createDataFrame(values_wide_pandas)

        # Fit models
        models = fit_models(budget_state_per_capital_spark)

        # Read population data
        population_prediction, population_historic = read_population_data()

        # Predict budget
        surgeries_pred_state = predict_budget(population_historic, population_prediction, models, budget_state_per_capital_spark)

        logging.info("Prediction process completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during the ML pipeline process: {e}")
