import streamlit as st
from pipeline_aprimorado import pipeline

if st.button('Processar'):
    with st.spinner('Processando...'):
        logs = pipeline()

        for log in logs:
            st.write(log)