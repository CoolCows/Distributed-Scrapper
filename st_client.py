from altair.vegalite.v4.api import value
import streamlit as st
from zmq.sugar.poll import Poller
from scrap_chord import ScrapChordClient
from threading import Thread

import zmq.sugar as zmq
from sortedcontainers import SortedSet

from utils.tools import zpipe


@st.cache(allow_output_mutation=True)
def create_chord_client(port, m):
    context = zmq.Context()
    pipe = zpipe(context)

    client = ScrapChordClient(port, m, pipe[0])
    t = Thread(target=client.run, daemon=True)
    t.start()

    return t, pipe[1]


if __name__ == "__main__":

    st.title("ScrapKord Client")
    st.sidebar.markdown("# Options")
    port = st.sidebar.text_input(value=8000, label="Port")
    bits = st.sidebar.text_input(value=32, label="Bits")
    show_html = st.sidebar.checkbox(value=False, label="Show html from page")
    show_urls_found = st.sidebar.checkbox(value=False, label="Show urls found")
    show_search_tree = st.sidebar.checkbox(value=True, label="Show Search Tree")
    st.sidebar.markdown("""#### Developed by CoolCows""")

    urls_req = st.text_input("Enter urls for scraping")
    start = st.button("Start")
    if start:
        t, chord_sock = create_chord_client(int(port), int(bits))
        done = False
        chord_sock.send_pyobj(urls_req)
        chord_sock.rcvtimeo = 8000
        count = 0
        key = 0
        while t.is_alive():
            try:
                obj = chord_sock.recv_pyobj()
                key += 1
                if len(obj) == 3:
                    url, html, url_list = obj
                    if not show_html and not show_urls_found:
                        st.text(f"Scraping({count})")
                        count += 1
                    if show_html or show_urls_found:
                        st.markdown(f"Scraped: {url}")
                    if show_html:
                        st.text("HTML:")
                        st.text(html)
                    if show_urls_found:
                        st.text("Links in page:")
                        st.text("\n".join(urlx for urlx in url_list))
                elif len(obj) == 2 and show_search_tree:
                    done = True
                    visual, url_html = obj
                    st.markdown("Search Tree Completed")
                    st.text(visual)
                    break

            except zmq.error.Again:
                if not done:
                    st.warning("No message from server")
                else:
                    st.text("")