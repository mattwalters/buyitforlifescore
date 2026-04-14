import sys
from unittest.mock import patch
import os

# mock input
@patch('builtins.input', side_effect=['2', '', 'n'])
def test_tui(mock_input):
    sys.path.insert(0, '/Users/matt/src/mattwalters/buyitforlifescore/app/pipeline/scripts')
    import eval_tui
    
    # We just want to see the model choice
    model = eval_tui.get_model()
    print(f"Model selected: {model}")
    thinking = eval_tui.get_thinking(model)
    print(f"Thinking selected: {thinking}")

test_tui()
