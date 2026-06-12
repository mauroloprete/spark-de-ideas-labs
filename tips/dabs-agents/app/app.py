"""
SoporteBot Chat UI — Streamlit app para Databricks Apps.

Se deploya como un Databricks App via DABs:

  apps:
    chat_ui:
      name: soporte-bot-chat-${bundle.target}
      source_code_path: ./app

La app conecta al serving endpoint del agente para responder preguntas.
"""

import os
import streamlit as st
from databricks.sdk import WorkspaceClient

st.set_page_config(page_title="SoporteBot", page_icon="🤖", layout="centered")

# --- Config ---
ENDPOINT_NAME = os.getenv("SERVING_ENDPOINT", "soporte-bot-dev")

# --- Init ---
w = WorkspaceClient()

# --- UI ---
st.title("🤖 SoporteBot")
st.caption("Asistente de soporte tecnico — powered by DABs + MLflow")

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

if prompt := st.chat_input("Escribi tu pregunta de soporte..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Buscando en la base de conocimiento..."):
            try:
                response = w.serving_endpoints.query(
                    name=ENDPOINT_NAME,
                    inputs=[{"question": prompt}],
                )
                answer = response.predictions[0]
            except Exception as e:
                answer = f"Error al consultar el agente: {str(e)}"

        st.markdown(answer)
        st.session_state.messages.append({"role": "assistant", "content": answer})

# --- Sidebar ---
with st.sidebar:
    st.markdown("### Sobre este lab")
    st.markdown(
        "Este chatbot es parte del lab "
        "[DABs + Agents](https://github.com/mauroloprete/spark-de-ideas-labs/tree/main/tips/dabs-agents)."
    )
    st.markdown(f"**Endpoint:** `{ENDPOINT_NAME}`")
    if st.button("Limpiar chat"):
        st.session_state.messages = []
        st.rerun()
