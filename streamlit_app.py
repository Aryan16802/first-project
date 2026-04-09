from __future__ import annotations

import os
from pathlib import Path

import streamlit as st

from mf_rag.ingestion.groww_client import SELECTED_GROWW_FUND_URLS
from mf_rag.phases.phase5 import RetrievalPipeline
from mf_rag.phases.phase6 import GroqClient, load_groq_config
from mf_rag.phases.phase7.service import ChatOrchestrator
from mf_rag.phases.phase9 import freshness_status
from mf_rag.storage.structured_store import StructuredStore
from mf_rag.storage.vector_store import InMemoryVectorStore


def _load_local_env() -> None:
    env_file = Path(".env")
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


@st.cache_resource
def _build_orchestrator() -> ChatOrchestrator:
    _load_local_env()
    store = StructuredStore(Path("data/truth.db"))
    store.init_schema()
    retrieval = RetrievalPipeline(structured_store=store, vector_store=InMemoryVectorStore())
    llm = GroqClient(load_groq_config())
    return ChatOrchestrator(retrieval=retrieval, llm_client=llm, store=store)


def _inject_css() -> None:
    st.markdown(
        """
        <style>
        .stApp { background: #0b1020; color: #e9ecf5; }
        .block-container { max-width: 1050px; padding-top: 1.0rem; padding-bottom: 1.0rem; }
        .title { font-size: 2rem; font-weight: 700; margin-bottom: 0.2rem; }
        .sub { color: #9fb0d8; margin-bottom: 1rem; }
        .chatbox { height: 520px; overflow-y: auto; background: #0f1628; border: 1px solid #2c395c; border-radius: 10px; padding: 12px; }
        .msg { margin: 10px 0; padding: 10px; border-radius: 10px; white-space: pre-wrap; }
        .user { background: #1f2c4b; border: 1px solid #304777; }
        .bot { background: #15243d; border: 1px solid #2d4f7a; }
        .meta { margin-top: 6px; font-size: 12px; color: #9fb0d8; }
        .fund-card { background: #0f1628; border: 1px solid #2c395c; border-radius: 8px; padding: 8px; margin-bottom: 8px; }
        .fund-link { color: #8fc1ff; text-decoration: none; word-break: break-all; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_chat(messages: list[dict[str, str]]) -> None:
    st.markdown('<div class="chatbox">', unsafe_allow_html=True)
    if not messages:
        st.markdown('<div class="msg bot">Ready. Ask about any of the listed mutual funds.</div>', unsafe_allow_html=True)
    for msg in messages:
        role = msg["role"]
        content = msg["content"]
        meta = msg.get("meta", "")
        cls = "user" if role == "user" else "bot"
        html = f'<div class="msg {cls}">{content}'
        if meta:
            html += f'<div class="meta">{meta}</div>'
        html += "</div>"
        st.markdown(html, unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


def main() -> None:
    st.set_page_config(page_title="Mutual Fund RAG Chatbot", page_icon="💬", layout="wide")
    _inject_css()

    st.markdown('<div class="title">Mutual Fund RAG Chatbot</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub">Ask scheme-level questions. Responses are grounded in indexed Groww sources.</div>',
        unsafe_allow_html=True,
    )

    left, right = st.columns([2.2, 1], gap="large")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    with right:
        st.markdown("#### 10 Selected Funds (Groww)")
        for url in SELECTED_GROWW_FUND_URLS:
            slug = url.rstrip("/").split("/")[-1].replace("-", " ")
            st.markdown(
                f'<div class="fund-card"><strong>{slug}</strong><br/><a class="fund-link" href="{url}" target="_blank">{url}</a></div>',
                unsafe_allow_html=True,
            )

    with left:
        _render_chat(st.session_state.messages)
        query = st.text_area(
            "Query",
            placeholder="Example: What is the NAV and AUM of UTI Nifty 50 Index Fund Direct Growth?",
            label_visibility="collapsed",
            height=90,
        )
        c1, c2 = st.columns([1, 1])
        send = c1.button("Send", use_container_width=True)
        clear = c2.button("Clear Chat", use_container_width=True)

        if clear:
            st.session_state.messages = []
            st.rerun()

        if send:
            query = query.strip()
            if not query:
                st.warning("Please type a question first.")
                st.stop()
            st.session_state.messages.append({"role": "user", "content": query})
            try:
                orchestrator = _build_orchestrator()
                result = orchestrator.chat(query)
                citation = result.get("citations", {}).get("source_url", "n/a")
                fresh = freshness_status(result.get("as_of"))
                meta = f"Source: {citation} | Freshness: {fresh.get('status', 'unknown')}"
                st.session_state.messages.append({"role": "bot", "content": result.get("answer", ""), "meta": meta})
            except Exception as exc:  # noqa: BLE001
                st.session_state.messages.append(
                    {
                        "role": "bot",
                        "content": "Request failed while generating response.",
                        "meta": f"Error: {exc}",
                    }
                )
            st.rerun()


if __name__ == "__main__":
    main()
