def show_shotness_stats(data):
    top = sorted(((r.counters[i], i) for i, r in enumerate(data)), reverse=True)
    for count, i in top:
        r = data[i]
        print("%8d  %s:%s [%s]" % (count, r.file, r.name, r.internal_role))
