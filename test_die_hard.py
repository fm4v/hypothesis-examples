from hypothesis import note, settings
from hypothesis.stateful import RuleBasedStateMachine, rule, invariant

from hypothesis import settings

settings.register_profile("ci", max_examples=5000)
settings.load_profile("ci")


# https://hypothesis.works/articles/how-not-to-die-hard-with-hypothesis/

class DieHardProblem(RuleBasedStateMachine):
    small = 0
    big = 0

    @rule()
    def fill_small(self):
        self.small = 3

    @rule()
    def fill_big(self):
        self.big = 5

    @rule()
    def empty_small(self):
        self.small = 0

    @rule()
    def empty_big(self):
        self.big = 0

    @rule()
    def pour_small_into_big(self):
        old_big = self.big
        self.big = min(5, self.big + self.small)
        self.small = self.small - (self.big - old_big)

    @rule()
    def pour_big_into_small(self):
        old_small = self.small
        self.small = min(3, self.small + self.big)
        self.big = self.big - (self.small - old_small)

    @invariant()
    def physics_of_jugs(self):
        assert 0 <= self.small <= 3
        assert 0 <= self.big <= 5

    @invariant()
    def die_hard_problem_not_solved(self):
        note("> small: {s} big: {b}".format(s=self.small, b=self.big))
        assert self.big != 4


DieHardTest = DieHardProblem.TestCase

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # The default of 200 is sometimes not enough for Hypothesis to find
    # a falsifying example.
    pass
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
