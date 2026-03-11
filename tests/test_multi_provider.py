"""
tests/test_multi_provider.py
-----------------------------
Unit tests for the multi-provider LLM fallback system and the pipeline wiring.

Tests cover:
  1. llm_service — _call_llm provider selection & fallback logic
  2. llm_service — schema serialisation helper
  3. langgraph_pipeline — WorkflowState has new keys; run_pipeline signature
  4. main.py — QueryRequest accepts new fields; _run_dashboard_query guard
  5. api_client — generate_dashboard / follow_up_query payload building
  6. Integration smoke — import chain from main app succeeds
"""

import json
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ROOT = __file__  # tests/test_multi_provider.py
# Make sure project root is on sys.path when running directly
import pathlib
sys.path.insert(0, str(pathlib.Path(ROOT).resolve().parent.parent))


# ===========================================================================
# 1. llm_service — _call_llm logic
# ===========================================================================

class TestCallLlmFallback(unittest.TestCase):
    """_call_llm should try providers in order and fall through on failure."""

    def _make_valid_json(self):
        return json.dumps({
            "sql_query": "SELECT 1",
            "chart_type": "bar",
            "x_axis": "x",
            "y_axis": "y",
            "title": "Test",
        })

    def test_no_keys_raises_quota_exceeded(self):
        from backend.llm_service import QuotaExceededError, _call_llm
        with self.assertRaises(QuotaExceededError):
            _call_llm("prompt", gemini_key="", openrouter_key="", groq_key="")

    def test_gemini_key_used_first(self):
        from backend.llm_service import _call_llm
        with patch("backend.llm_service._call_gemini_provider", return_value=self._make_valid_json()) as mock_g, \
             patch("backend.llm_service._call_openai_compatible") as mock_or:
            result = _call_llm("prompt", gemini_key="gkey", openrouter_key="orkey", groq_key="")
        mock_g.assert_called_once()
        mock_or.assert_not_called()
        self.assertEqual(result["chart_type"], "bar")

    def test_falls_back_to_openrouter_when_gemini_fails(self):
        from backend.llm_service import _call_llm
        valid = self._make_valid_json()
        with patch("backend.llm_service._call_gemini_provider", side_effect=RuntimeError("quota")), \
             patch("backend.llm_service._call_openai_compatible", return_value=valid) as mock_or:
            result = _call_llm("prompt", gemini_key="gkey", openrouter_key="orkey", groq_key="")
        mock_or.assert_called_once()
        self.assertEqual(result["sql_query"], "SELECT 1")

    def test_falls_back_to_groq_when_gemini_and_openrouter_fail(self):
        from backend.llm_service import _call_llm, _GROQ_BASE, _GROQ_MODEL
        valid = self._make_valid_json()

        call_count = {"n": 0}

        def side_effect(prompt, api_key, base_url, model):
            call_count["n"] += 1
            if base_url == _GROQ_BASE:
                return valid
            raise RuntimeError("quota")

        with patch("backend.llm_service._call_gemini_provider", side_effect=RuntimeError("quota")), \
             patch("backend.llm_service._call_openai_compatible", side_effect=side_effect):
            result = _call_llm("prompt", gemini_key="gkey", openrouter_key="orkey", groq_key="groqkey")
        self.assertEqual(result["sql_query"], "SELECT 1")

    def test_all_providers_fail_raises_quota_exceeded(self):
        from backend.llm_service import QuotaExceededError, _call_llm
        with patch("backend.llm_service._call_gemini_provider", side_effect=RuntimeError("quota")), \
             patch("backend.llm_service._call_openai_compatible", side_effect=RuntimeError("rate limit")):
            with self.assertRaises(QuotaExceededError):
                _call_llm("p", gemini_key="g", openrouter_key="o", groq_key="q")

    def test_only_groq_key_skips_gemini_and_openrouter(self):
        from backend.llm_service import _call_llm
        valid = self._make_valid_json()
        with patch("backend.llm_service._call_gemini_provider") as mock_g, \
             patch("backend.llm_service._call_openai_compatible", return_value=valid) as mock_or:
            result = _call_llm("prompt", gemini_key="", openrouter_key="", groq_key="groqkey")
        mock_g.assert_not_called()
        mock_or.assert_called_once()  # called once for Groq
        self.assertIn("sql_query", result)

    def test_unsupported_query_raises_value_error(self):
        from backend.llm_service import _call_llm
        bad = json.dumps({"error": "UNSUPPORTED_QUERY"})
        with patch("backend.llm_service._call_gemini_provider", return_value=bad):
            with self.assertRaises(ValueError, msg="UNSUPPORTED_QUERY"):
                _call_llm("prompt", gemini_key="gkey")

    def test_non_json_response_raises_value_error(self):
        from backend.llm_service import _call_llm
        with patch("backend.llm_service._call_gemini_provider", return_value="not json at all"):
            with self.assertRaises(ValueError):
                _call_llm("prompt", gemini_key="gkey")

    def test_markdown_fenced_json_is_stripped(self):
        from backend.llm_service import _call_llm
        fenced = "```json\n" + self._make_valid_json() + "\n```"
        with patch("backend.llm_service._call_gemini_provider", return_value=fenced):
            result = _call_llm("prompt", gemini_key="gkey")
        self.assertIn("sql_query", result)


# ===========================================================================
# 2. llm_service — schema serialisation
# ===========================================================================

class TestSchemaToString(unittest.TestCase):
    def test_basic_schema(self):
        from backend.llm_service import _schema_to_string
        schema = {
            "revenue": {"dtype": "float64", "null_count": 0, "unique_count": 100,
                        "sample_values": [1.0, 2.0, 3.0]},
            "product": {"dtype": "object", "null_count": 2, "unique_count": 5,
                        "sample_values": ["A", "B"]},
        }
        result = _schema_to_string(schema)
        self.assertIn("revenue", result)
        self.assertIn("product", result)
        self.assertIn("float64", result)

    def test_sample_values_capped_at_5(self):
        from backend.llm_service import _schema_to_string
        schema = {
            "col": {"dtype": "int64", "null_count": 0, "unique_count": 20,
                    "sample_values": list(range(20))},
        }
        result = _schema_to_string(schema)
        # Only first 5 values should appear
        self.assertIn("4", result)      # index 4 is in first 5
        self.assertNotIn("19", result)  # index 19 should be trimmed


# ===========================================================================
# 3. langgraph_pipeline — WorkflowState & run_pipeline signature
# ===========================================================================

class TestWorkflowState(unittest.TestCase):
    def test_workflow_state_has_new_keys(self):
        from backend.langgraph_pipeline import WorkflowState
        hints = WorkflowState.__annotations__
        self.assertIn("openrouter_key", hints)
        self.assertIn("groq_key", hints)
        self.assertIn("api_key", hints)

    def test_run_pipeline_accepts_new_params(self):
        import inspect
        from backend.langgraph_pipeline import run_pipeline
        sig = inspect.signature(run_pipeline)
        params = list(sig.parameters.keys())
        self.assertIn("openrouter_key", params)
        self.assertIn("groq_key", params)

    def test_run_pipeline_passes_keys_to_initial_state(self):
        """Verify run_pipeline threads all keys into the initial state."""
        from backend.langgraph_pipeline import run_pipeline
        captured = {}

        def fake_invoke(state):
            captured.update(state)
            return {**state, "status": "success", "insights": [], "business_recommendations": []}

        with patch("backend.langgraph_pipeline._compiled_graph") as mock_graph:
            mock_graph.invoke = fake_invoke
            run_pipeline("test q", api_key="gkey", openrouter_key="orkey", groq_key="groqkey")

        self.assertEqual(captured.get("api_key"), "gkey")
        self.assertEqual(captured.get("openrouter_key"), "orkey")
        self.assertEqual(captured.get("groq_key"), "groqkey")


# ===========================================================================
# 4. main.py — QueryRequest & _run_dashboard_query guard
# ===========================================================================

class TestQueryRequest(unittest.TestCase):
    def test_query_request_has_new_fields(self):
        from backend.main import QueryRequest
        req = QueryRequest(question="test", api_key="g", openrouter_key="o", groq_key="q")
        self.assertEqual(req.openrouter_key, "o")
        self.assertEqual(req.groq_key, "q")

    def test_query_request_new_fields_default_to_empty(self):
        from backend.main import QueryRequest
        req = QueryRequest(question="test question")
        self.assertEqual(req.openrouter_key, "")
        self.assertEqual(req.groq_key, "")

    def test_run_dashboard_query_allows_if_only_groq_key(self):
        """Guard should pass if ANY key is set, not just Gemini."""
        from backend.main import _run_dashboard_query
        with patch("backend.main.get_active_schema", return_value={"col": {}}), \
             patch("backend.main.run_pipeline", return_value={
                 "status": "success", "sql_query": "SELECT 1", "chart_type": "bar",
                 "chart_figure": {}, "insights": [], "business_recommendations": [],
             }):
            resp = _run_dashboard_query("q", api_key="", openrouter_key="", groq_key="groqkey")
        self.assertEqual(resp.status, "success")

    def test_run_dashboard_query_rejects_all_empty_keys(self):
        from fastapi import HTTPException
        from backend.main import _run_dashboard_query
        with self.assertRaises(HTTPException) as ctx:
            _run_dashboard_query("q", api_key="", openrouter_key="", groq_key="")
        self.assertEqual(ctx.exception.status_code, 400)

    def test_run_dashboard_query_uses_all_three_keys(self):
        """run_pipeline must be called with all three keys."""
        from backend.main import _run_dashboard_query
        with patch("backend.main.get_active_schema", return_value={"col": {}}), \
             patch("backend.main.run_pipeline", return_value={
                 "status": "success", "sql_query": "SELECT 1", "chart_type": "bar",
                 "chart_figure": {}, "insights": [], "business_recommendations": [],
             }) as mock_pipeline:
            _run_dashboard_query("q", api_key="g", openrouter_key="o", groq_key="q2")

        mock_pipeline.assert_called_once_with(
            "q", api_key="g", openrouter_key="o", groq_key="q2"
        )


# ===========================================================================
# 5. api_client — payload building
# ===========================================================================

class TestApiClientPayload(unittest.TestCase):
    def _mock_response(self, data: dict):
        mock = MagicMock()
        mock.raise_for_status = MagicMock()
        mock.json.return_value = data
        return mock

    def test_generate_dashboard_sends_all_keys(self):
        from frontend.utils.api_client import generate_dashboard
        success_data = {"status": "success", "question": "q", "insights": []}
        with patch("requests.post", return_value=self._mock_response(success_data)) as mock_post:
            generate_dashboard("q", api_key="g", openrouter_key="o", groq_key="gr")
        _, kwargs = mock_post.call_args
        payload = kwargs["json"]
        self.assertEqual(payload["api_key"], "g")
        self.assertEqual(payload["openrouter_key"], "o")
        self.assertEqual(payload["groq_key"], "gr")

    def test_follow_up_query_sends_all_keys(self):
        from frontend.utils.api_client import follow_up_query
        success_data = {"status": "success", "question": "q", "insights": []}
        with patch("requests.post", return_value=self._mock_response(success_data)) as mock_post:
            follow_up_query("q", api_key="g", openrouter_key="or", groq_key="gq")
        _, kwargs = mock_post.call_args
        payload = kwargs["json"]
        self.assertEqual(payload["openrouter_key"], "or")
        self.assertEqual(payload["groq_key"], "gq")

    def test_generate_dashboard_defaults_to_empty_strings(self):
        from frontend.utils.api_client import generate_dashboard
        success_data = {"status": "success", "question": "q", "insights": []}
        with patch("requests.post", return_value=self._mock_response(success_data)) as mock_post:
            generate_dashboard("q")
        _, kwargs = mock_post.call_args
        payload = kwargs["json"]
        self.assertEqual(payload["openrouter_key"], "")
        self.assertEqual(payload["groq_key"], "")


# ===========================================================================
# 6. Integration smoke — import chain
# ===========================================================================

class TestImportSmoke(unittest.TestCase):
    def test_llm_service_imports(self):
        import backend.llm_service as svc  # noqa: F401
        self.assertTrue(hasattr(svc, "generate_sql_and_chart_config"))
        self.assertTrue(hasattr(svc, "generate_insights_and_recommendations"))
        self.assertTrue(hasattr(svc, "QuotaExceededError"))

    def test_pipeline_imports(self):
        import backend.langgraph_pipeline as pl  # noqa: F401
        self.assertTrue(hasattr(pl, "run_pipeline"))
        self.assertTrue(hasattr(pl, "WorkflowState"))

    def test_main_imports(self):
        import backend.main as m  # noqa: F401
        self.assertTrue(hasattr(m, "app"))
        self.assertTrue(hasattr(m, "QueryRequest"))

    def test_api_client_imports(self):
        import frontend.utils.api_client as ac  # noqa: F401
        self.assertTrue(hasattr(ac, "generate_dashboard"))
        self.assertTrue(hasattr(ac, "follow_up_query"))

    def test_public_function_signatures(self):
        import inspect
        from backend.llm_service import (
            generate_insights_and_recommendations,
            generate_sql_and_chart_config,
        )
        for fn in (generate_sql_and_chart_config, generate_insights_and_recommendations):
            params = inspect.signature(fn).parameters
            self.assertIn("openrouter_key", params, f"{fn.__name__} missing openrouter_key")
            self.assertIn("groq_key", params, f"{fn.__name__} missing groq_key")


if __name__ == "__main__":
    unittest.main(verbosity=2)
