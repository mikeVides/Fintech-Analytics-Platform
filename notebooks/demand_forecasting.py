# Databricks notebook source
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

pd.set_option('display.float_format', '{:.1f}'.format)

print('Setup complete.')

# COMMAND ----------

df_loans = spark.sql("""
    SELECT
        application_date AS ds,
        COUNT(*) AS y
    FROM default.fct_loan_applications
    WHERE application_date IS NOT NULL
    GROUP BY application_date
    ORDER BY application_date
""").toPandas()

df_loans['ds'] = pd.to_datetime(df_loans['ds'])
df_loans['y'] = df_loans['y'].astype(float)

print(f'Date range:     {df_loans.ds.min().date()} to {df_loans.ds.max().date()}')
print(f'Total rows:     {len(df_loans):,}')
print(f'Average daily:  {df_loans.y.mean():.1f} applications')
print(f'Min daily:      {df_loans.y.min():.1f} applications')
print(f'Max daily:      {df_loans.y.max():.1f} applications')
print()
print(df_loans.head(10))

# COMMAND ----------

train_end = '2025-08-31'
test_start = '2025-09-01'

df_train = df_loans[df_loans['ds'] <= train_end].copy()
df_test  = df_loans[df_loans['ds'] >= test_start].copy()

print(f'Training set:   {df_train.ds.min().date()} to {df_train.ds.max().date()} ({len(df_train):,} days)')
print(f'Test set:       {df_test.ds.min().date()} to {df_test.ds.max().date()} ({len(df_test):,} days)')

# COMMAND ----------

model = Prophet(
    yearly_seasonality=True,
    weekly_seasonality=True,
    daily_seasonality=False,
    changepoint_prior_scale=0.05
)

model.fit(df_train)

print('Model training complete.')
print(f'Trained on {len(df_train):,} days of loan application data.')

# COMMAND ----------

future = model.make_future_dataframe(periods=180)

forecast = model.predict(future)

print(f'Forecast generated for {len(future):,} dates')
print(f'Future periods:  180 days beyond training data')
print()
print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(10))

# COMMAND ----------

df_evaluation = df_test.merge(
    forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']],
    on='ds',
    how='left'
)

df_evaluation['error'] = df_evaluation['y'] - df_evaluation['yhat']
df_evaluation['abs_error'] = df_evaluation['error'].abs()
df_evaluation['pct_error'] = (df_evaluation['abs_error'] / df_evaluation['y']) * 100
df_evaluation['squared_error'] = df_evaluation['error'] ** 2

mape = df_evaluation['pct_error'].mean()
rmse = np.sqrt(df_evaluation['squared_error'].mean())

print(f'Forecast Accuracy Metrics')
print(f'─' * 35)
print(f'MAPE:   {mape:.2f}%')
print(f'RMSE:   {rmse:.2f} applications')
print(f'MAE:    {df_evaluation["abs_error"].mean():.2f} applications')
print()
print(df_evaluation[['ds', 'y', 'yhat', 'error', 'pct_error']].head(10))

# COMMAND ----------

fig = model.plot(forecast)

plt.title('Loan Application Volume — Prophet Forecast', fontsize=14, fontweight='bold')
plt.xlabel('Date', fontsize=11)
plt.ylabel('Daily Applications', fontsize=11)

from matplotlib.lines import Line2D
from matplotlib.patches import Patch

legend_elements = [
    Line2D([0], [0], marker='o', color='w', markerfacecolor='black',
           markersize=6, label='Actual applications (historical)'),
    Line2D([0], [0], color='steelblue', linewidth=2,
           label="Prophet forecast (yhat)"),
    Patch(facecolor='lightblue', alpha=0.5,
          label='80% uncertainty interval'),
]

plt.legend(handles=legend_elements, loc='upper left', fontsize=9)

plt.figtext(0.5, -0.02,
    'Black dots = actual daily loan applications. Blue line = Prophet prediction. '
    'Shaded area = 80% confidence band (yhat_lower to yhat_upper).',
    wrap=True, horizontalalignment='center', fontsize=8, color='gray')

plt.tight_layout()
plt.show()

# COMMAND ----------

fig2 = model.plot_components(forecast)
plt.suptitle('Prophet Forecast Components', 
             fontsize=14, fontweight='bold', y=1.02)
plt.tight_layout()
plt.show()

# COMMAND ----------

df_evaluation['month'] = df_evaluation['ds'].dt.to_period('M')

monthly_accuracy = df_evaluation.groupby('month').agg(
    actual_total    = ('y',         'sum'),
    forecast_total  = ('yhat',      'sum'),
    mae             = ('abs_error', 'mean'),
    mape            = ('pct_error', 'mean')
).reset_index()

monthly_accuracy['variance'] = (
    monthly_accuracy['actual_total'] - monthly_accuracy['forecast_total']
)
monthly_accuracy['variance_pct'] = (
    monthly_accuracy['variance'] / monthly_accuracy['actual_total'] * 100
)

print('Monthly Forecast Accuracy — Test Period')
print('─' * 65)
print(monthly_accuracy.to_string(index=False))
print()
print(f'Best month:   {monthly_accuracy.loc[monthly_accuracy.mape.idxmin(), "month"]} (MAPE: {monthly_accuracy.mape.min():.1f}%)')
print(f'Worst month:  {monthly_accuracy.loc[monthly_accuracy.mape.idxmax(), "month"]} (MAPE: {monthly_accuracy.mape.max():.1f}%)')

# COMMAND ----------

forecast_output = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()

forecast_output = forecast_output[forecast_output['ds'] >= '2025-09-01']

forecast_output.columns = ['forecast_date', 'predicted_applications', 
                            'predicted_lower', 'predicted_upper']

forecast_output['predicted_applications'] = forecast_output['predicted_applications'].clip(lower=0).round(2)
forecast_output['predicted_lower']        = forecast_output['predicted_lower'].clip(lower=0).round(2)
forecast_output['predicted_upper']        = forecast_output['predicted_upper'].clip(lower=0).round(2)

forecast_output['model_version']  = 'prophet_v1'
forecast_output['trained_through'] = '2025-08-31'
forecast_output['created_at']      = pd.Timestamp.now()

forecast_spark = spark.createDataFrame(forecast_output)

forecast_spark.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable('default.srv_loan_forecast')

print(f'Serving table created: default.srv_loan_forecast')
print(f'Rows written: {forecast_output.shape[0]:,}')
print()
print(forecast_output.head(5))

# COMMAND ----------

spark.sql("SELECT * FROM default.srv_loan_forecast LIMIT 5").show()