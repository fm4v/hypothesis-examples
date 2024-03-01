import pytest

from hypothesis import strategies as st, settings
from hypothesis.stateful import RuleBasedStateMachine, rule, Bundle, run_state_machine_as_test


class ListStateMachine(RuleBasedStateMachine):
    list_ = Bundle('list')

    @rule(target=list_, items=st.lists(st.integers(min_value=0, max_value=100), max_size=5))
    def create_list(self, items):
        return items

    @rule(list_=list_, item=st.integers())
    def append_item(self, list_, item):
        list_.append(item)
        assert list_[-1] == item

    @rule(list_=list_)
    def pop_item(self, list_):
        if len(list_) > 0:
            pop_item = list_[-1]
            item = list_.pop()
            # The popped item should be removed from the list
            assert (not list_ or item == pop_item)
        else:
            # no elements in list
            with pytest.raises(IndexError):
                list_.pop()

    @rule(target=list_, list1=list_, list2=list_)
    def add_two_lists(self, list1, list2):
        list3 = list1 + list2

        # Check two lists is appended to each other
        assert len(list1) + len(list2) == len(list3)
        assert list3[0:len(list1)] == list1
        assert list3[len(list1):len(list1) + len(list2)] == list2

        return list3


def test_run():
    settings_ = settings(
        deadline=None,
        stateful_step_count=15,
        max_examples=1000
    )

    run_state_machine_as_test(ListStateMachine, settings=settings_)
