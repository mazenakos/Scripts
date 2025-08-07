#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 24 11:12:56 2024

@author: mazin.almaqablah
"""

import pickle


with open('all_params.pkl', 'rb') as f:
    data = pickle.load(f)
    
data.to_csv('all_params.csv', index = False)