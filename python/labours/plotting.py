import os
from pathlib import Path


def import_pyplot(backend, style):
    import matplotlib

    if backend:
        matplotlib.use(backend)
    from matplotlib import pyplot

    pyplot.style.use(style)
    print("matplotlib: backend is", matplotlib.get_backend())
    return matplotlib, pyplot


def apply_plot_style(figure, axes, legend, background, font_size, axes_size):
    foreground = "black" if background == "white" else "white"
    if axes_size is None:
        axes_size = (16, 12)
    else:
        axes_size = tuple(float(p) for p in axes_size.split(","))
    figure.set_size_inches(*axes_size)
    for side in ("bottom", "top", "left", "right"):
        axes.spines[side].set_color(foreground)
    for axis in (axes.xaxis, axes.yaxis):
        axis.label.update(dict(fontsize=font_size, color=foreground))
    for axis in ("x", "y"):
        getattr(axes, axis + "axis").get_offset_text().set_size(font_size)
        axes.tick_params(axis=axis, colors=foreground, labelsize=font_size)
    try:
        axes.ticklabel_format(axis="y", style="sci", scilimits=(0, 3))
    except AttributeError:
        pass
    figure.patch.set_facecolor(background)
    axes.set_facecolor(background)
    if legend is not None:
        frame = legend.get_frame()
        for setter in (frame.set_facecolor, frame.set_edgecolor):
            setter(background)
        for text in legend.get_texts():
            text.set_color(foreground)


def get_plot_path(base: str, name: str) -> str:
    root, ext = os.path.splitext(base)
    if not ext:
        ext = ".png"
    output = os.path.join(root, name + ext)
    os.makedirs(os.path.dirname(output), exist_ok=True)
    return output


def deploy_plot(title: str, output: str, background: str, tight: bool = True) -> None:
    import matplotlib.pyplot as pyplot

    if not output:
        pyplot.gcf().canvas.set_window_title(title)
        pyplot.show()
    else:
        po = Path(output)
        if len(po.name) > 64:
            suffix = po.suffix[:5]
            output = str(po.with_name(po.stem[:64 - len(suffix)] + suffix))
        if title:
            pyplot.title(title, color="black" if background == "white" else "white")
        if tight:
            try:
                pyplot.tight_layout()
            except:  # noqa: E722
                print("Warning: failed to set the tight layout")
        print("Writing plot to %s" % output)
        pyplot.savefig(output, transparent=True)
    pyplot.clf()
