import random
import itertools


class idgen:
    '''Generates a random id composed of [a-zA-Z0-9] characters by default'''
    len: int

    _composition: list[str]
    _cdomain: list[str]
    def __init__(self, len: int) -> None:
        self.len = len
        self._composition = [
            "a-z",
            "A-Z",
            "0-9"
        ]
        self._set_character_domain()

    def generate(self) -> str:
        I_min = 0
        I_max = len(self._cdomain) - 1
        retval = ""
        for i in range(self.len):
            retval += self._cdomain[random.randint(I_min, I_max)]
        return retval


    def _set_character_domain(self) -> str:
        self._cdomain = []
        for comp in self._composition:
            self._cdomain.append( 
                [ chr(x) for x in range( ord(comp[0]), ord(comp[-1]) + 1) ] 
            )
        self._cdomain = [ x for x in itertools.chain(*self._cdomain) ]

if __name__ == "__main__":
    id = idgen(20)
    print(id.generate())