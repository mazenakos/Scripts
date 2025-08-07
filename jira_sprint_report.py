#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul  8 11:12:27 2024

@author: mazin.almaqablah
"""

import pandas as pd
import numpy as np

start=13

combined_df = pd.DataFrame()
for i in range(18):
    j = i + start
    print('JIRA-' + str(j))
    df = pd.read_csv('JIRA-' + str(j) + '.csv')
    #sprint = input('Enter Sprint ID to Analyze: ')
    df.dropna(how='all', axis=1, inplace=True)
    df.drop('Status', axis=1, inplace=True)
    #df = df.loc[df['Sprint'] == 'CTET ' + sprint]
    df = df.loc[df['Issue Type'] == 'Task']

    print('\n')
    print('JIRA-' + str(j))
    print('Sprint: ' + df.Sprint.describe().top)
    print('Total # of Issues: ' + str(df.count().iloc[0]))
    print('Total # Story Points: ' + str(df['Custom field (Story Points).3'].sum()))
    print('Number of Participants: ' +str(df.Assignee.nunique()))

    # Calculate percent completion by issue:
    issue_percent = 100*(df.loc[df['Status Category'] == 'Done'].count().iloc[0])/(df.count().iloc[0])
    issue_percent = np.round(issue_percent, decimals=0, out=None)
    print('Percent Completion Issue: ' + str(issue_percent))

    # Calculate percent completion by story points:
    sp_percent = (100*(df.groupby(['Status Category'])['Custom field (Story Points).3'].sum().iloc[0])/(df['Custom field (Story Points).3'].sum()))
    sp_percent = np.round(sp_percent, decimals=0, out=None)
    print('Percent Completion Story Points: ' + str(sp_percent))

    # Calculate % Completion by team members
    ## step1: find total count of tickets by member
    df1 = df.groupby(['Assignee'])['Status Category'].describe()
    df1.drop(['unique', 'top', 'freq'], axis=1, inplace=True)

    # step2: get count of Done tasks by member
    df2 = df.loc[df['Status Category'] == 'Done']
    df2 = df2.groupby(['Assignee'])['Status Category'].describe()
    df2.drop(['count', 'unique', 'top'], axis=1, inplace=True)
    df2.rename({'freq': 'count_done'}, axis=1, inplace=True)

    # step3: get count of To-Do tasks by member
    df3 = df.loc[df['Status Category'] == 'To Do']
    df3 = df3.groupby(['Assignee'])['Status Category'].describe()
    df3.drop(['count', 'unique', 'top'], axis=1, inplace=True)
    df3.rename({'freq': 'count_todo'}, axis=1, inplace=True)

    # step4: get count of In-Progress tasks by member
    df4 = df.loc[df['Status Category'] == 'In Progress']
    df4 = df4.groupby(['Assignee'])['Status Category'].describe()
    df4.drop(['count', 'unique', 'top'], axis=1, inplace=True)
    df4.rename({'freq': 'count_inprogress'}, axis=1, inplace=True)

    # step5: join all dataframes for the final team level percentage calculation
    df5 = pd.merge(pd.merge(df1,df2,on='Assignee', how='outer'),df3,on='Assignee', how='outer')
    df6 = pd.merge(df4,df5,on='Assignee', how='outer')
    df6 = df6.fillna(0)


####### ADD Story Points Sum per team member
    # step6:calculate team level percent completion 
    df6['Completion %'] = 100*df6['count_done']/df6['count']
    df6['Sprint Complete?'] = np.where(df6['Completion %'] >= 90, 'Yes', 'No')
    team_percent = 100*(df6.loc[df6['Sprint Complete?'] == 'Yes'].count().iloc[0])/(df6.count().iloc[0])
    team_percent = np.round(team_percent, decimals=0, out=None)
    sorted_df = df6.sort_values(by=['Completion %'], ascending = False)
    sorted_df = sorted_df.round(1)
    sorted_df['Sprint'] = df.Sprint.describe().top
    combined_df = pd.concat([combined_df, sorted_df])
    print('Percent Completion Team: ' + str(team_percent))

combined_df.to_csv('combined_sorted_df.csv')