Hercules
--------

This tool calculates the weekly lines burnout in a Git repository.

###Usage

```
hercules https://github.com/src-d/go-git | python3 labours.py
hercules /path/to/cloned/go-git | python3 labours.py
hercules https://github.com/torvalds/linux /tmp/linux_cache | python3 labours.py
git rev-list HEAD | tac | hercules -commits -sampling 7 - https://github.com/src-d/go-git | python3 labours.py
```

###License
MIT.
