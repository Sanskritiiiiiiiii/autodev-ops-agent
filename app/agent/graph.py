from langgraph.graph import END, START, StateGraph

from app.agent.state import LLM, AgentState
from app.agent.tools import build_diff_preview, check_fix, retrieve_code_context


def build_graph(llm: LLM, retriever, max_attempts: int = 3):
    def analyze_log(state: AgentState) -> dict:
        out = llm.analyze(state["log"])
        return {"hypothesis": out["hypothesis"], "search_query": out["search_query"]}

    def retrieve_code(state: AgentState) -> dict:
        return {"retrieved_chunks": retrieve_code_context(retriever, state["search_query"])}

    def draft_fix(state: AgentState) -> dict:
        out = llm.draft_fix(state["log"], state["hypothesis"],
                            state["retrieved_chunks"], state.get("verification_feedback"))
        return {
            "hypothesis": out.get("hypothesis", state["hypothesis"]),
            "proposed_fix": {"chunk_id": out.get("chunk_id"), "new_code": out.get("new_code")},
            "attempts": state["attempts"] + 1,
        }

    def verify(state: AgentState) -> dict:
        result = check_fix(state["proposed_fix"], state["retrieved_chunks"])
        if result["ok"]:
            return {"evaluation_status": "passed", "verification_feedback": None}
        return {"evaluation_status": "failed",
                "verification_feedback": "; ".join(result["errors"])}

    def output_diff(state: AgentState) -> dict:
        target = next(c for c in state["retrieved_chunks"]
                      if c["chunk_id"] == state["proposed_fix"]["chunk_id"])
        return {"diff_preview": build_diff_preview(target, state["proposed_fix"]["new_code"])}

    def route_after_verify(state: AgentState) -> str:
        if state["evaluation_status"] == "passed":
            return "output_diff"
        if state["attempts"] < max_attempts:
            return "draft_fix"
        return "end"

    g = StateGraph(AgentState)
    g.add_node("analyze_log", analyze_log)
    g.add_node("retrieve_code", retrieve_code)
    g.add_node("draft_fix", draft_fix)
    g.add_node("verify", verify)
    g.add_node("output_diff", output_diff)

    g.add_edge(START, "analyze_log")
    g.add_edge("analyze_log", "retrieve_code")
    g.add_edge("retrieve_code", "draft_fix")
    g.add_edge("draft_fix", "verify")
    g.add_conditional_edges("verify", route_after_verify,
                            {"output_diff": "output_diff", "draft_fix": "draft_fix", "end": END})
    g.add_edge("output_diff", END)
    return g.compile()