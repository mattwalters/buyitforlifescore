"""Tests for the entity triage prompt and schema."""

from pipeline.prompts.entity_triage import TriageDecision, get_entity_triage_prompt


class TestGetEntityTriagePrompt:
    def test_raw_mention_appears_in_prompt(self):
        prompt = get_entity_triage_prompt(
            raw_mention="Darn Tough socks",
            text="I've had mine for 6 years.",
            parent_text="Best hiking socks?",
        )
        assert "Darn Tough socks" in prompt

    def test_text_appears_in_prompt(self):
        prompt = get_entity_triage_prompt(
            raw_mention="Lodge cast iron",
            text="Mine is absolutely bulletproof after 10 years.",
            parent_text="",
        )
        assert "Mine is absolutely bulletproof after 10 years." in prompt

    def test_parent_text_appears_in_prompt(self):
        prompt = get_entity_triage_prompt(
            raw_mention="Vitamix",
            text="Still going strong.",
            parent_text="Title: Best blenders?\nBody: Looking for something durable.",
        )
        assert "Title: Best blenders?" in prompt

    def test_pass_and_fail_criteria_present(self):
        prompt = get_entity_triage_prompt(
            raw_mention="Leatherman",
            text="Daily carry for 20 years.",
            parent_text="",
        )
        assert "PASS" in prompt
        assert "FAIL" in prompt

    def test_troubleshooting_with_embedded_signal_covered(self):
        """The troubleshooting edge case must be explicitly called out in the prompt."""
        prompt = get_entity_triage_prompt(
            raw_mention="Vitamix",
            text="My Vitamix died, how do I fix it?",
            parent_text="",
        )
        # The prompt must instruct the model that failure-embedded-in-question is a PASS
        assert "question" in prompt.lower()
        assert "failure" in prompt.lower() or "broke" in prompt.lower() or "repair" in prompt.lower()

    def test_empty_parent_text_handled(self):
        prompt = get_entity_triage_prompt(
            raw_mention="Red Wing boots",
            text="12 years and still going.",
            parent_text="",
        )
        assert isinstance(prompt, str)
        assert len(prompt) > 0


class TestTriageDecisionSchema:
    def test_valid_pass_decision(self):
        decision = TriageDecision(passes=True, reasoning="User explicitly states 6 years of ownership.")
        assert decision.passes is True
        assert len(decision.reasoning) > 0

    def test_valid_fail_decision(self):
        decision = TriageDecision(passes=False, reasoning="Pure question with no embedded signal.")
        assert decision.passes is False

    def test_passes_is_required(self):
        import pytest

        with pytest.raises(Exception):
            TriageDecision(reasoning="Missing passes field.")

    def test_reasoning_is_required(self):
        import pytest

        with pytest.raises(Exception):
            TriageDecision(passes=True)
