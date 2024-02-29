import os
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
def stat():
    # list to store files
    res = []
    dir_path= r'./pdfs'
    count = 0
    columns = ['folder_name','count_files']
    list_of_lists = []
    for root_dir, cur_dir, files in os.walk(dir_path):
        list_of_lists.append([root_dir[7:],len(files)])
        count += len(files)
    df = pd.DataFrame(list_of_lists,columns=columns)
    df.to_csv('current_stat.csv')
    #create bar graph
    fig = go.Figure(go.Bar(
            x=df['folder_name'],
            y=df['count_files'],
            orientation='v'))
    fig.show()
    #print(df)

if __name__=="__main__":
    stat()