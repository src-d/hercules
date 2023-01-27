from scipy.signal import windows


def old_slepian(m, bw):
    nw = bw * m / 4
    return windows.dpss(m, nw)
