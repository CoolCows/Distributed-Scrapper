import streamlit as st
from scrap_chord import ScrapChordClient
from threading import Thread

import zmq.sugar as zmq
from sortedcontainers import SortedSet

from utils.tools import (
    register_socks,
    zpipe,
)

@st.cache(allow_output_mutation=True)
def create_chord_client(port, m):
    context = zmq.Context()
    pipe = zpipe(context)

    client = ScrapChordClient(port, m, pipe[0])
    Thread(target=client.run, daemon=True).start()

    return pipe[1]


if __name__ == "__main__":

    st.title("ScrapKord Client")
    st.sidebar.markdown("# Options")
    port = st.sidebar.text_input(value=8000, label="Port")
    bits = st.sidebar.text_input(value=5, label="Bits")
    show_urls_found = st.sidebar.checkbox(value=True, label="Show urls found")
    st.sidebar.markdown("""#### Developed by CoolCows""")

    pipe = create_chord_client(int(port), int(bits))
    urls_req = st.text_input("Enter urls for scraping")
    start = st.button("Start")
    if start:
        pipe.send_pyobj(urls_req)

    poller = zmq.Poller()
    register_socks(poller, pipe)
    ready = dict(poller.poll(timeout=1000))
    if pipe in ready:
        url, html, url_list = pipe.recv_pyobj()
        st.markdown("URL")
        st.text(url)

        st.markdown("HTML")
        st.text(html)
        if show_urls_found:
            st.markdown("URLs found")
            st.text(url_list)
