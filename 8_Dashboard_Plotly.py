from plotly.subplots import make_subplots
import plotly.graph_objs as go
from pathlib import Path
import pandas as pd
import warnings


figs = []
bands = []
band_L09 = []
band_L18 = []
band_L21 = []

with warnings.catch_warnings():
    warnings.simplefilter("ignore")

    with open('output.html', 'w') as f:
        f.write('')

    # Add the columns required
    columns_required = ['Date', 
               'Time', 
               'Cell Name', 
               'Total Traffic (Mbit)_LRNO (MB)', 
               'User Throughput in DownLink (Mbps)_N_LRNO (number)',
               'User Throughput in DownLink (Mbps)_D_LRNO (number)', 
               'User Throughput in UpLink (Mbps)_N_LRNO (number)',
               'User Throughput in UpLink (Mbps)_D_LRNO (number)',
               'DL PDCP Throughput (Mbit/s)_N_LRNO (Mbit/s)',
               'DL PDCP Throughput (Mbit/s)_D_LRNO (Mbit/s)',
               'UL PDCP Throughput (Mbit/s)_N_LRNO (Mbit/s)',
               'UL PDCP Throughput (Mbit/s)_D_LRNO (number)']

    # Load CSV in DataFrame
    path = Path(__file__).parent.absolute()
    name = f"{path}\\Combine\\super_merge.csv"
    df = pd.read_csv(name, usecols=columns_required)

    df = df.replace('', 0.0)

    df['Band'] = df['Cell Name'].str[7:10]
    unique_bands = df.groupby('Band')['Band'].unique()

    band_list = []
    for i in unique_bands:
        band_list.extend(i.tolist())


    column_calculations = ['Total Data Volume(4G)', 
                    'DL User ThrpT_Mbps(4G)', 
                    'UL User ThrpT_Mbps(4G)', 
                    'DL PDCP ThrpT_Mbps', 
                    'UL PDCP ThrpT_Mbps']
    


    def plot_grouped_data(grouped_df, column, title):
        fig = go.Figure()
        for name, group in grouped_df:
            group.set_index('Time', inplace=True)
            fig.add_trace(go.Scatter(x=group.index, y=group[column], name=name, mode='lines',  line=dict(shape='spline', smoothing=0.7)))

        fig.update_layout(
            xaxis=dict(
                tickangle=270,
                ),
            xaxis_title='Time',
            yaxis_title=column,
            title=title,
            margin=dict(l=50,r=50,b=50,t=50,pad=4),
            width=700,
            height=400,
            plot_bgcolor='white',
            paper_bgcolor='white',
            shapes=[
                dict(
                    type='rect',
                    xref='paper',
                    yref='paper',
                    x0=0,
                    y0=0,
                    x1=1,
                    y1=1,
                    line=dict(color='black', width=3),
                    fillcolor='rgba(0,0,0,0)',
                    layer='below',
                )
            ]
        )

        return fig


    
    def calc_1(df_test, column, figs):
        df_test = df_test[['Date', 'Time', column]]
        grouped_df = df_test.groupby('Date')
        fig = plot_grouped_data(grouped_df, column, column)
        fig.update_layout(template='simple_white')
        figs.append(fig)
        del df_test
        del grouped_df

    def calc_2(df_test, filtered_df, band_L09, band_L18, band_L21):
        df_test = df_test[['Date', 'Time', column]]
        grouped_df = df_test.groupby('Date')
        fig = plot_grouped_data(grouped_df, column, f"{column} {band_list[element]}")
        fig.update_layout(template='simple_white')

        if band_list[element] == 'L09':
            band_L09.append(fig)
        elif band_list[element] == 'L18':
            band_L18.append(fig)
        elif band_list[element] == 'L21':
            band_L21.append(fig)
        del grouped_df
        del filtered_df


    for column in column_calculations[:]:
        if column == 'Total Data Volume(4G)':
            df_test = df.sort_values(['Date','Time']).groupby(['Date','Time']).agg(
                col = ('Total Traffic (Mbit)_LRNO (MB)', lambda x: x.sum() / 8 / 1024 / 1024 / 1024 / 1024)
            ).reset_index().rename(columns={'col': column})

            calc_1(df_test, column, figs)

            for element in range(len(band_list)): 
                filtered_df = df[df['Band'].isin([band_list[element]])]
                df_test = filtered_df.sort_values(['Date','Time']).groupby(['Date','Time']).agg(
                    col = ('Total Traffic (Mbit)_LRNO (MB)', lambda x: x.sum() / 8 / 1024 / 1024 / 1024 / 1024)
                ).reset_index().rename(columns={'col': column})

                calc_2(df_test, filtered_df, band_L09, band_L18, band_L21)
        
        # END OF A COLUMN CALCULATION 


        # START OF A COLUMN CALCULATION 

        elif column == 'DL User ThrpT_Mbps(4G)':
            df_test = df.sort_values(['Date','Time']).groupby(['Date','Time']).agg(
                col_a = ('User Throughput in DownLink (Mbps)_N_LRNO (number)', lambda x: x.sum()),
                col_b = ('User Throughput in DownLink (Mbps)_D_LRNO (number)', lambda x: x.sum())).reset_index()
                
            df_test[column]=df_test['col_a']/df_test['col_b']
            calc_1(df_test, column, figs)

            for element in range(len(band_list)): 
                filtered_df = df[df['Band'].isin([band_list[element]])]
                df_test = filtered_df.sort_values(['Date','Time']).groupby(['Date','Time']).agg(
                    col_a = ('User Throughput in DownLink (Mbps)_N_LRNO (number)', lambda x: x.sum()),
                    col_b = ('User Throughput in DownLink (Mbps)_D_LRNO (number)', lambda x: x.sum())).reset_index()
                    
                df_test[column]=df_test['col_a']/df_test['col_b']
                calc_2(df_test, filtered_df, band_L09, band_L18, band_L21)

        # END OF A COLUMN CALCULATION 


        # START OF A COLUMN CALCULATION 

        elif column == 'UL User ThrpT_Mbps(4G)':
            df_test = df.sort_values(['Date','Time']).groupby(['Date','Time']).agg(
                col_a = ('User Throughput in UpLink (Mbps)_N_LRNO (number)', lambda x: x.sum()),
                col_b = ('User Throughput in UpLink (Mbps)_D_LRNO (number)', lambda x: x.sum())).reset_index()
                
            df_test[column]=df_test['col_a']/df_test['col_b']
            calc_1(df_test, column, figs)

            for element in range(len(band_list)):
                filtered_df = df[df['Band'].isin([band_list[element]])]           
                df_test = filtered_df.sort_values(['Date','Time']).groupby(['Date','Time']).agg(
                    col_a = ('User Throughput in UpLink (Mbps)_N_LRNO (number)', lambda x: x.sum()),
                    col_b = ('User Throughput in UpLink (Mbps)_D_LRNO (number)', lambda x: x.sum())).reset_index()
                    
                df_test[column]=df_test['col_a']/df_test['col_b']
                calc_2(df_test, filtered_df, band_L09, band_L18, band_L21)

        # END OF A COLUMN CALCULATION 


        # START OF A COLUMN CALCULATION 

        elif column == 'DL PDCP ThrpT_Mbps':
            df_test = df.sort_values(['Date','Time']).groupby(['Date','Time']).agg(
                col_a = ('DL PDCP Throughput (Mbit/s)_N_LRNO (Mbit/s)', lambda x: x.sum()),
                col_b = ('DL PDCP Throughput (Mbit/s)_D_LRNO (Mbit/s)', lambda x: x.sum())).reset_index()
            
            df_test[column]=df_test['col_a']/df_test['col_b']
            calc_1(df_test, column, figs)

            for element in range(len(band_list)):
                filtered_df = df[df['Band'].isin([band_list[element]])]           
                df_test = filtered_df.sort_values(['Date','Time']).groupby(['Date','Time']).agg(
                    col_a = ('DL PDCP Throughput (Mbit/s)_N_LRNO (Mbit/s)', lambda x: x.sum()),
                    col_b = ('DL PDCP Throughput (Mbit/s)_D_LRNO (Mbit/s)', lambda x: x.sum())).reset_index()
                    
                df_test[column]=df_test['col_a']/df_test['col_b']
                calc_2(df_test, filtered_df, band_L09, band_L18, band_L21)

        # END OF A COLUMN CALCULATION 


        # START OF A COLUMN CALCULATION 

        elif column == 'UL PDCP ThrpT_Mbps':
            df_test = df.sort_values(['Date','Time']).groupby(['Date','Time']).agg(
                col_a = ('UL PDCP Throughput (Mbit/s)_N_LRNO (Mbit/s)', lambda x: x.sum()),
                col_b = ('UL PDCP Throughput (Mbit/s)_D_LRNO (number)', lambda x: x.sum())).reset_index()
            
            df_test[column]=df_test['col_a']/df_test['col_b']
            calc_1(df_test, column, figs)

            for element in range(len(band_list)):
                filtered_df = df[df['Band'].isin([band_list[element]])]           
                df_test = filtered_df.sort_values(['Date','Time']).groupby(['Date','Time']).agg(
                    col_a = ('UL PDCP Throughput (Mbit/s)_N_LRNO (Mbit/s)', lambda x: x.sum()),
                    col_b = ('UL PDCP Throughput (Mbit/s)_D_LRNO (number)', lambda x: x.sum())).reset_index()
                    
                df_test[column]=df_test['col_a']/df_test['col_b']
                calc_2(df_test, filtered_df, band_L09, band_L18, band_L21)


    # HTML PART STARTING

    div = "<div style='display:flex; flex-wrap:wrap; justify-content:center;'>"
    for i, fig in enumerate(figs):
        div += f"<div style='flex:1; max-width:50%; margin:10px;'>{fig.to_html(full_html=False, include_plotlyjs='cdn')}</div>"
    div += "</div>"

    with open('output.html', 'w') as f:
        f.write(f"<html><head><br><p><h1><b>NETWORK</b></h1></p><br><style>img {{max-width:100%; height:auto;}} @media screen and (min-width: 768px) {{ div > div {{max-width:50%;}} }}</style></head><body>{div}</body></html>")

    def generate_band_html(Item, List, Name):
        if Item in band_list:
            div = "<div style='display:flex; flex-wrap:wrap; justify-content:center;'>"
            for i, band in enumerate(List):
                div += f"<div style='flex:1; max-width:50%; margin:10px;'>{band.to_html(full_html=False, include_plotlyjs='cdn')}</div>"
            div += "</div>"

            with open('output.html', 'a') as f:
                f.write(f"<html><head><br><p><h1><b>{Name}</b><h1></p><br><style>img {{max-width:100%; height:auto;}} @media screen and (min-width: 768px) {{ div > div {{max-width:50%;}} }}</style></head><body>{div}</body></html>")

    generate_band_html('L09', band_L09, 'L900')
    generate_band_html('L18', band_L18, 'L1800')
    generate_band_html('L09', band_L21, 'L2100')
