import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.impute import SimpleImputer
from datetime import datetime
import tensorflow as  tf
from sklearn.metrics import mean_absolute_error, mean_squared_error

data = pd.read_csv(r'C:\Users\anhad\Downloads\updated(aug10).csv')

# Data preprocessing
data.dropna(subset=['CPUTime', 'Memory', 'NCpus', 'VMem', 'Walltime'], inplace=True)
data['CPUTime'] = pd.to_timedelta(data['CPUTime']).dt.total_seconds()
data['Memory'] = data['Memory'].str.replace('kb', '').astype(np.int64)
data['VMem'] = data['VMem'].str.replace('kb', '').str.replace(',', '').astype(np.int64)
data['Walltime'] = pd.to_timedelta(data['Walltime']).dt.total_seconds()
data['Start_Time'] = pd.to_datetime(data['Start_Time'])
data['Ctime'] = pd.to_datetime(data['Ctime'])
data['Waiting_Time'] = (data['Start_Time'] - data['Ctime']).dt.total_seconds()
features = ['CPUTime', 'Memory', 'NCpus', 'VMem', 'Walltime', 'Resource_List_ncpus', 'Resource_List_ngpus', 'Waiting_Time']
X = data[features]
y = data['Waiting_Time']
X = X.dropna()
y = y[X.index]
imputer = SimpleImputer(strategy='mean')
X_imputed = imputer.fit_transform(X)
X_train, X_test, y_train, y_test = train_test_split(X_imputed, y, test_size=0.8, random_state=42)
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Make predictions on the test set
y_pred = model.predict(X_test)

# Calculate evaluation metrics
mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
rmse = np.sqrt(mse)
print(mae)
print(mse)
print(rmse)

# Load the new data for prediction
new_data = pd.read_csv(r'C:\Users\anhad\Desktop\intenship\extraction\test.csv')
new_data['CPUTime'] = pd.to_timedelta(new_data['CPUTime']).dt.total_seconds()
new_data['Waiting_Time'] = 0
new_data['Memory'] = new_data['Memory'].str.replace('kb', '') 
new_data['Memory'] = pd.to_numeric(new_data['Memory'], errors='coerce')
new_data['VMem'] = new_data['VMem'].fillna('0').str.replace('kb', '').str.replace(',', '').astype(np.int64)
new_data['Walltime'] = pd.to_timedelta(new_data['Walltime']).dt.total_seconds()

# Calculate the waiting time based on 'Ctime'
current_time = datetime.now()
new_data['Ctime'] = pd.to_datetime(new_data['Ctime'], format="%a %b %d %H:%M:%S %Y")
new_data['Waiting_Time'] = (current_time - new_data['Ctime']).dt.total_seconds()

features = ['CPUTime', 'Memory', 'NCpus', 'VMem', 'Walltime', 'Resource_List_ncpus', 'Resource_List_ngpus', 'Waiting_Time']
X_predict = new_data[features]

# Impute missing values in the new data
imputer = SimpleImputer(strategy='mean')
X_predict_imputed = imputer.fit_transform(X_predict)

# Make predictions using the trained model
predictions = model.predict(X_predict_imputed)

#estimated waiting time
new_data['Predicted_Waitingtime'] = predictions

#estimated remaining waiting time
new_data['Estimated_Remaining_Wait'] = new_data['Predicted_Waitingtime'] - new_data['Waiting_Time']

print(new_data[['Memory', 'NCpus', 'VMem','Ctime', 'Walltime', 'Resource_List_ncpus', 'Predicted_Waitingtime', 'Estimated_Remaining_Wait']])

# Save the modified DataFrame to a CSV file
new_data.to_csv('latest', index=False)