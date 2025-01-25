from app import app
from flask import render_template, jsonify
from flask import request

import pandas as pd
from datetime import datetime, timedelta
import calendar

#--------------------------------------------

#   DESHBOARD PAGE 
@app.route('/')
def home():


    return render_template('index.html')

# -------------------------------------------------


# -------------------------------------------------
#     ALL P & L PAGE

# -------------------------------------------------


# -------------------------------------------------
#     TRADE LOG BOOK TABLE

# -------------------------------------------------
#     MARKET DATA CHARTS and PAGES FOR OTHER OPTIONS (Update Data & Add Symbol Data)

# -------------------------------------------------


# -------------------------------------------------
#  STATICAL REPORT

# ----------------------------------------------------------------



# CALENDAR VIEW


# ----------------------

# --------------------------------------------------------

# --------------------------------------------------------



from app.exch_data import download_nse_bse_files_1, last_business_day, updates_dates, get_postgresdb_symbols
from app.nse_data_study_8 import update_database_function

@app.route('/utility_exch',methods=['GET', 'POST'])
def utility_exch():
        last_business_date  = last_business_day()
        all_symbols = get_postgresdb_symbols()
        if request.method == 'POST':
            if 'today' in request.form:
                today = datetime.now().strftime('%d/%m/%Y')
                start_date = today
                end_date = today
                print(f'Data will be downloaded from {start_date} to {end_date}.')
                status = download_nse_bse_files_1(start_date, end_date)

            elif 'range' in request.form:
                start_date = request.form['start_date']
                end_date = request.form['end_date']
                print(f'Data will be downloaded from {start_date} to {end_date}.')
                status = download_nse_bse_files_1(start_date, end_date)

        
            elif 'update_dates' in request.form:
                new_dates = request.form.get('dates').strip().splitlines()
                if not new_dates or new_dates == ['']:  # Check for empty input
                    print("No dates were provided.")
                else:
                    print(f"New dates received: {new_dates}")
                    status = updates_dates(new_dates)
            # Handle the database update case
            elif 'update_range' in request.form:
                update_start_date = request.form['update_start_date']
                update_end_date = request.form['update_end_date']
                print(f'Database will be updated from {update_start_date} to {update_end_date}.')
                status = update_database_function(update_start_date, update_end_date)  # Replace with your actual function
            elif 'select_company' in request.form:
                selected_comp = request.form['symbol']
                print(selected_comp)
            elif 'symbol' in request.form:
                 symbol = request.form.get('symbol')
                 print(f'Selected symbol is {symbol}.')
                

        return render_template('utility_exch.html',last_business_date =last_business_date, all_symbols=all_symbols )
        # Here you can add your logic to handle the data download based on the dates
        # return f'Data will be downloaded from {start_date} to {end_date}.'  
    
# # --------------------------------------------------------

# --------------------------------------------------------
from app.exch_data import get_postgresdb_data
from app.exch_data import get_financial_data
@app.route('/basic_analysis_n',  methods=['GET', 'POST'])
def basic_analysis_n():
    all_symbols = get_postgresdb_symbols()
    if 'symbol' in request.form:
        symbol = request.form.get('symbol')
        print(f'Selected symbol is {symbol}.')
    else:
        # symbol = 'ICICIBANK :- ICICI BANK LTD.  INE090A01021'
        symbol = 'ONGC :- OIL AND NATURAL GAS CORP.  INE213A01029'
    
    
    isin_daily_df, script_name = get_postgresdb_data(symbol)
    
    qtr_data, yearly_data = get_financial_data(symbol)
    qtr_data_html = qtr_data.head(2).to_html(classes='data', header=True, index=False)
    yearly_data_html = yearly_data.head(2).to_html(classes='data', header=True, index=False)


    return render_template('basic_analysis_n.html',isin_daily_df = isin_daily_df.to_dict(orient='records'), all_symbols = all_symbols, script_name=script_name, qtr_data=qtr_data_html,yearly_data=yearly_data_html)
# -------------------------------------------------------------------

