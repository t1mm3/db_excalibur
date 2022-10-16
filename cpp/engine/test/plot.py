#!/bin/env python

import matplotlib as mpl
mpl.use('pgf')
pgf_with_pgflatex = {
    "pgf.texsystem": "pdflatex",
    "pgf.rcfonts": False,
    #"pgf.preamble": [
    #     r"\usepackage[utf8x]{inputenc}",
    #     r"\usepackage[T1]{fontenc}",
    #     # r"\usepackage{cmbright}",
    #     ]
}
mpl.rcParams.update(pgf_with_pgflatex)
mpl.rcParams['axes.axisbelow'] = True

kibi = 1024.0
mebi = kibi*1024.0
gibi = mebi*1024.0

kilo = 1000.0
mega = kilo * 1000.0
giga = mega * 1000.0

import matplotlib.pyplot as plt

import pandas as pd
import numpy as np
import math
import matplotlib.ticker as mticker
from matplotlib.ticker import MultipleLocator, FuncFormatter

prop_cycle = plt.rcParams['axes.prop_cycle']
colors = prop_cycle.by_key()['color']
hatches = ["//", "--", "\\\\", "xx", "||", "++"]
markers = ["o", "x", "s", "+", "^", "v"]

result_path = "@CMAKE_CURRENT_BINARY_DIR@"

framework_columns_sel = ["host", "git_sha", "run_id", "name", "sel", "qname", "threads", "t_all",
    "t_qmean", "t_qmin", "t_qmax", "t_qmedian"]
framework_columns = ["host", "git_sha", "run_id", "name", "qname", "threads", "t_all",
    "t_qmean", "t_qmin", "t_qmax", "t_qmedian"]
framework_columns_q6 = ["host", "git_sha", "run_id", "name", "variant",
    "discount", "year", "quantity", "qname", "threads", "t_all",
    "t_qmean", "t_qmin", "t_qmax", "t_qmedian"]

def plot_q1sel(prefix):
    file1 = "{}/{}.csv".format(result_path, prefix)
    print("Reading file {}".format(file1))
    df = pd.read_csv(file1,
        sep='|', names=framework_columns_sel, header=None)

    df = df[df["sel"] != 99]
    print(df)

    df=df.sort_values(by=['sel'])


    (fig, ax1) = plt.subplots()

    ax1.set_ylabel('Time (ms)')
    ax1.set_xlabel('Selectivity (\\%)')

    filter_names = ["interpret", "statements", "adaptive-h", "adaptive-r"]
    legend_names = ["vectorized", "data-centric", "adapt (heur)", "adapt (rand)"]
    filter_dfs = map(lambda n: df[df['name'] == n], filter_names)

    zipped = list(zip(legend_names, filter_dfs))

    index = 0
    for (descr, frame) in zipped:
        print(descr)

        with pd.option_context('display.max_rows', None, 'display.max_columns', 100):
            print(frame)
        ax1.plot(frame['sel'], frame['t_qmean'],
            linestyle='--', marker=markers[index], color=colors[index], label=descr)

        index += 1

    box = ax1.get_position()
    ax1.set_position([box.x0, box.y0 + box.height * 0.1,
                     box.width, box.height * 0.9])

    # Put a legend below current axis
    #legend = ax1.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2),
    #          fancybox=False, ncol=3)
    ax1.legend(loc='lower right', ncol=1)

    fig.tight_layout()
    #,legend2
    fig.savefig("{}.pgf".format(prefix), bbox_extra_artists=(), bbox_inches='tight')
    plt.close(fig)

def qnum_from_str(x):
    s = x[1:]
    try:
        return int(s)
    except ValueError:
        return -1

def unique_values(df, col):
    values = []

    for index, row in df.iterrows():
        x = row[col]
        if x not in values:
            values.append(x)

    return values

def plot_qperf(prefix, sort=True, rotateX=0, filterPattern="", moveLegend=0, seconds=False, errRange=False):
    file1 = "{}/{}.csv".format(result_path, prefix)
    print("Reading file {}".format(file1))
    df = pd.read_csv(file1,
        sep='|', names=framework_columns, header=None)

    if sort:
        df['qnum'] = df['qname'].apply(lambda x: qnum_from_str(x))
        print(df)

        df=df.sort_values(by=['qnum'])

    if len(filterPattern)>0:
        df=df[df['qname'].str.contains(filterPattern)]

    all_queries = unique_values(df, "qname")
    print(all_queries)

    (fig, ax1) = plt.subplots()

    ax1.set_ylabel('Time (s)' if seconds else 'Time (ms)')
    # ax1.set_xlabel('Selectivity (\\%)')

    filter_names = ["interpret", "statements",
        "adaptive-h", "adaptive-r",
        # "adaptive-r2",
        # "adaptive-rm",
        "adaptive-mcts"]
    legend_names = ["vectorized", "data-centric",
        "adapt (heur)", "adapt (rand)",
        #"adapt (r2)",
        # "adapt (rm)",
        "adapt (mcts)"]
    adaptive = [False, False, True, True, True, True, True]
    filter_dfs = map(lambda n: df[df['name'] == n], filter_names)

    zipped = list(zip(legend_names, filter_dfs, adaptive))

    num_subbars = len(zipped)
    total_bar_width = 0.75

    X = np.arange(len(all_queries))
    ax1.set_xticks(X)

    index = 0
    for (descr, frame, is_adapt) in zipped:
        print(descr)

        w = total_bar_width / float(num_subbars)

        with pd.option_context('display.max_rows', None, 'display.max_columns', 100):
            print(frame)

        div = 1000.0 if seconds else 1.0
        adj = 1.5*w

        if errRange:
            yvalues = frame['t_qmedian']/div

            yerr = [np.subtract(yvalues, frame['t_qmin']/div),
            np.subtract(frame['t_qmax']/div, yvalues)]

            print(descr)

            ax1.bar(X + index * w - adj, yvalues,
                width=w, color=colors[index], label=descr,
                yerr=yerr, capsize=2)
        else:
            ax1.bar(X + index * w -adj, frame['t_qmin']/div,
                width=w, color=colors[index], label=descr)

        index += 1

    # plt.xticks(ticks=X, labels=all_queries)

    labels = []
    for l in all_queries:
        labels.append(l)
        print(l)
    ax1.set_xticklabels(labels, rotation=rotateX)

    box = ax1.get_position()
    ax1.set_position([box.x0, box.y0 + box.height * 0.1,
                     box.width, box.height * 0.9])

    # Put a legend below current axis
    #legend = ax1.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2),
    #          fancybox=False, ncol=3)
    if moveLegend == 1:
        ax1.legend(bbox_to_anchor=(0, 1, 1, 0), loc='lower left', mode='expand', ncol=2)
    elif moveLegend == 2:
        ax1.legend(loc='upper center', ncol=2)
    else:
        ax1.legend(loc='upper left', ncol=2)

    fig.tight_layout()
    #,legend2
    print("{}{}.pgf".format(prefix,filterPattern))
    fig.savefig("{}{}.pgf".format(prefix,filterPattern), bbox_extra_artists=(), bbox_inches='tight')
    plt.close(fig)


def plot_q6(prefix, variant=0, giveDisc = None, giveYear = None, giveQuant = None,
        rotateX=0, moveLegend=0, seconds=False, errRange=False, legend=False):
    file1 = "{}/{}.csv".format(result_path, prefix)
    print("Reading file {}".format(file1))
    df = pd.read_csv(file1,
        sep='|', names=framework_columns_q6, header=None)

    num_given = 0
    if giveDisc is not None:
        num_given += 1
    if giveQuant is not None:
        num_given += 1
    if giveYear is not None:
        num_given += 1

    assert(num_given == 2)
    (fig, ax1) = plt.subplots()
    print(df)
    maxTime = max(df["t_qmedian"])
    print(maxTime)

    dim = None
    if giveDisc is None:
        dim = "discount"
        ax1.set_xlabel(' DISCOUNT')
    elif giveQuant is None:
        dim = "quantity"
        ax1.set_xlabel('QUANTITY')
    elif giveYear is None:
        dim = "year"
        ax1.set_xlabel('YEAR')
    else:
        assert(False)

    df = df[df['variant'] == variant]

    if giveDisc is not None:
        df = df[df['discount'] == giveDisc]
    if giveQuant is not None:
        df = df[df['quantity'] == giveQuant]
    if giveYear is not None:
        df = df[df['year'] == giveYear]

    print(df)

    all_queries = unique_values(df, dim)
    print(all_queries)

   

    ax1.set_ylabel('Time (s)' if seconds else 'Time (ms)')
    # ax1.set_xlabel('Selectivity (\\%)')

    filter_names = ["interpret", "statements",
        "adaptive-h", "adaptive-r",
        # "adaptive-r2",
        # "adaptive-rm",
        "adaptive-mcts"]
    legend_names = ["vectorized", "data-centric",
        "adapt (h)", "adapt (rand)",
        #"adapt (r2)",
        #"adapt (rm)",
        "adapt(mcts)"]
    adaptive = [False, False, True, True, True, True, True]
    filter_dfs = map(lambda n: df[df['name'] == n], filter_names)

    zipped = list(zip(legend_names, filter_dfs, adaptive))

    num_subbars = len(zipped)
    total_bar_width = 0.75

    X = np.arange(len(all_queries))
    ax1.set_xticks(X)
    if maxTime is not None:
        ax1.set_ylim(ymin=0, ymax=maxTime)

    index = 0
    for (descr, frame, is_adapt) in zipped:
        if len(frame) < 1:
            continue
        print(descr)

        w = total_bar_width / float(num_subbars)

        with pd.option_context('display.max_rows', None, 'display.max_columns', 100):
            print(frame)

        div = 1000.0 if seconds else 1.0
        adj = 1.5*w

        if errRange:
            yvalues = frame['t_qmedian']/div

            yerr = [np.subtract(yvalues, frame['t_qmin']/div),
            np.subtract(frame['t_qmax']/div, yvalues)]

            print(descr)

            ax1.bar(X + index * w - adj, yvalues,
                width=w, color=colors[index], label=descr,
                yerr=yerr, capsize=2)
        else:
            ax1.bar(X + index * w -adj, frame['t_qmin'],
                width=w, color=colors[index], label=descr)

        index += 1

    # plt.xticks(ticks=X, labels=all_queries)

    labels = []
    for l in all_queries:
        labels.append(l)
        print(l)
    ax1.set_xticklabels(labels, rotation=rotateX)

    box = ax1.get_position()
    ax1.set_position([box.x0, box.y0 + box.height * 0.1,
                     box.width, box.height * 0.9])

    if legend:
        if moveLegend == 1:
            ax1.legend(bbox_to_anchor=(0, 1, 1, 0), loc='lower left', mode='expand', ncol=2)
        elif moveLegend == 2:
            ax1.legend(loc='upper center', ncol=2)
        else:
            ax1.legend(loc='upper left', ncol=2)

    fig.tight_layout()
    #,legend2
    fig.savefig("{}_{}_{}_{}_{}.pgf".format(
        prefix,giveYear,giveQuant,giveDisc,variant),
        bbox_extra_artists=(), bbox_inches='tight')
    plt.close(fig)

def plot_qrisk(prefix, query, learn):
    file1 = "{}/{}.csv".format(result_path, prefix)
    print("Reading file {}".format(file1))
    df = pd.read_csv(file1,
        sep='|', names=framework_columns_sel, header=None)

    df = df[df["qname"] == query]
    print(df)

    df['qnum'] = df['qname'].apply(lambda x: qnum_from_str(x))
    print(df)

    df=df.sort_values(by=['sel'])
    df=df[df['sel'] < 0.6]
    df=df[df['sel'] > 0]

    all_budgets = unique_values(df, "sel")
    print(all_budgets)

    (fig, ax1) = plt.subplots()

    ax1.set_ylabel('Time (s)')
    # ax1.set_yticks(np.arange(0, 1600, step=200))
    if query != "q9":
       plt.ylim(0, 1.600)
    # ax1.set_xlabel('Selectivity (\\%)')

    filter_names = ["interpret", "statements",
        "adaptive-h" if not learn else "adaptive-h-learn",
        "adaptive-r" if not learn else "adaptive-r-learn",
        # "adaptive-r2" if not learn else "adaptive-r2-learn",
        # "adaptive-rm" if not learn else "adaptive-rm-learn",
        "adaptive-mcts" if not learn else "adaptive-mcts-learn"
        ]
    legend_names = ["vectorized", "data-centric",
        "adapt (heur)", "adapt (rand)",
        # "adapt (r2)",
        "adapt (mcts)",
        # "adapt (rm)"
        ]
    adaptive = [False, False, True, True, True, True, True]
    filter_dfs = map(lambda n: df[df['name'] == n], filter_names)

    zipped = list(zip(legend_names, filter_dfs, adaptive))

    num_subbars = len(zipped)
    total_bar_width = 0.75

    X = np.arange(len(all_budgets))
    ax1.set_xticks(X)

    index = 0
    for (descr, frame, is_adapt) in zipped:
        print(descr)

        w = total_bar_width / float(num_subbars)

        with pd.option_context('display.max_rows', None, 'display.max_columns', 100):
            print(frame)

        yvalues = frame['t_qmedian']

        yerr = [np.subtract(yvalues, frame['t_qmin']) / 1000.0,
            np.subtract(frame['t_qmax'], yvalues) / 1000.0]

        adj = 1.5*w
        ax1.bar(X + index * w - adj, yvalues/1000.0,
            width=w, color=colors[index], label=descr,
            yerr=yerr, capsize=2)

        index += 1

    # plt.xticks(ticks=X, labels=all_queries)

    labels = []
    for l in all_budgets:
        labels.append("{}\\%".format(int(l*100)))
    ax1.set_xticklabels(labels)

    box = ax1.get_position()
    ax1.set_position([box.x0, box.y0 + box.height * 0.1,
                     box.width, box.height * 0.9])

    # Put a legend below current axis
    #legend = ax1.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2),
    #          fancybox=False, ncol=3)
    if learn:
      ax1.legend(loc='lower center', ncol=2, columnspacing=0.4, labelspacing=0.2, handletextpad=0.4, framealpha=0.9)
    # columnspacing=0.2
    if learn:
      ax1.get_yaxis().set_visible(False)

    fig.tight_layout()
    #,legend2
    postfix = ""
    if learn:
        postfix = postfix + "_learn"
    fig.savefig("{}_{}{}.pgf".format(prefix, query, postfix),
        bbox_extra_artists=(), bbox_inches='tight')
    plt.close(fig)

def plot_adapt(prefix, query, learn, seconds=False):
    file1 = "{}/{}.csv".format(result_path, prefix)
    print("Reading file {}".format(file1))
    df = pd.read_csv(file1,
        sep='|', names=framework_columns, header=None)

    df = df[df["qname"] == query]
    print(df)

    df['qnum'] = df['qname'].apply(lambda x: qnum_from_str(x))
    print(df)

    df=df.sort_values(by=['run_id'])

    (fig, ax1) = plt.subplots()

    ax1.set_ylabel('Time (s)' if seconds else 'Time (ms)')
    # ax1.set_xlabel('Selectivity (\\%)')

    filter_names = ["interpret", "statements",
        "adaptive-h" if not learn else "adaptive-h-learn",
        "adaptive-r" if not learn else "adaptive-r-learn",
        # "adaptive-r2" if not learn else "adaptive-r2-learn",
        # "adaptive-rm" if not learn else "adaptive-rm-learn",
        "adaptive-mcts" if not learn else "adaptive-mcts-learn"]
    legend_names = ["vectorized", "data-centric",
        "adapt (heur)", "adapt (rand)",
        # "adapt (r2)",
        "adapt (rm)", "adapt (mcts)"]
    adaptive = [False, False, True, True, True, True, True]
    filter_dfs = map(lambda n: df[df['name'] == n], filter_names)

    zipped = list(zip(legend_names, filter_dfs, adaptive))

    for (descr, frame, is_adapt) in zipped:
        print(descr)

        with pd.option_context('display.max_rows', None, 'display.max_columns', 100):
            print(frame)

        div = 1000.0 if seconds else 1.0

        yvalues = frame['t_qmin']/div
        xvalues = frame['run_id']

        ax1.plot(xvalues, yvalues, label=descr)

    box = ax1.get_position()
    ax1.set_position([box.x0, box.y0 + box.height * 0.1,
                     box.width, box.height * 0.9])

    # Put a legend below current axis
    #legend = ax1.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2),
    #          fancybox=False, ncol=3)
    ax1.legend(loc='upper right', ncol=2)

    fig.tight_layout()
    #,legend2
    postfix = ""
    if learn:
        postfix = postfix + "_learn"
    fig.savefig("{}_{}{}.pgf".format(prefix, query, postfix),
        bbox_extra_artists=(), bbox_inches='tight')
    plt.close(fig)

def qspeed_text(text):
    text.set_text(str(int(text.get_text()) % 100))
    return text

def plot_speed(prefix, query, settings_id):
    plt.rcParams.update({
    "font.family": "serif",  # use serif/main font for text elements
    "text.usetex": True,     # use inline math for ticks
    "pgf.rcfonts": False     # don't setup fonts from rc parameters
    })


    file1 = "{}/{}.csv".format(result_path, prefix)
    print("Reading file {}".format(file1))
    df = pd.read_csv(file1,
        sep='|', names=["settings_id", "hostname", "query_id",
            "stream", "stage_id", "progress",
            "win_min_cyc_prog", "win_cur_cyc_prog",
            "measured_cyc_prog", "new_budget", "actions", "p_diff",
            "timestamp_rdtsc", "curr_cyc_tup", "min_cyc_tup", "rows"], header=None)

    (fig, ax1) = plt.subplots()

    df = df[df['query_id'] == query]
    # df = df[df['settings_id'] == settings_id]
    df.fillna('', inplace=True)

    ax1.set_ylabel('Cycles/Row')
    ax1.set_xlabel('Query Progress (\\%) in each Pipeline')

    stages = unique_values(df, "stage_id")
    index = 0

    num_stages = len(stages)
    stage_index = 0

    labels = []
    xticks = []
    for i in range(0, num_stages):
        for l in [0, 25, 50, 75]:
            labels.append(str(l))
            xticks.append(100*i + l)
    # ax1.set_xticks(xticks, labels=labels)
    ax1.set_xticks(xticks)
    ax1.set_xticklabels(labels)

    for stage in stages:
        stage_df = df[df['stage_id'] == stage]

        if len(stage_df) < 10:
            # not enough samples
            continue

        stage_df=stage_df.sort_values(by=['progress'])

        if False:
            with pd.option_context('display.max_rows', None, 'display.max_columns', 100):
                print(stage_df)

        # ("rm", 3)
        for (settings_name, settings_id) in [("heur", 1), ("mcts", 5)]:
            sdf = stage_df[stage_df['settings_id'] == settings_id]
            xvalues = sdf["progress"]*100.0 + 100.0*stage_index
            yvalues = sdf["min_cyc_tup"] # measured_cyc_prog
            ax1.plot(xvalues, yvalues, color=colors[index % len(colors)],
                label="P" + str(stage) + " ({})".format(settings_name))
            index+= 1
        stage_index += 1

        if False:
            yvalues = stage_df["win_cur_cyc_prog"]
            ax1.plot(xvalues, yvalues, color=colors[index],
                label="Stage {} curr win".format(stage))
            
            index+= 1

            yvalues = stage_df["win_min_cyc_prog"]
            ax1.plot(xvalues, yvalues, color=colors[index],
                label="Stage {} min win".format(stage))
            
            index+= 1

        if False:
            # annotate
            actions = []
            actions_keys = []
            for row_idx, row in stage_df.iterrows():
                x = row["actions"]
                if len(x) == 0:
                    x = 'base'
                if x not in actions_keys:
                    actions.append((x, row["progress"],
                        row["measured_cyc_prog"]))
                    actions_keys.append(x)

            action_idx = 0
            for (action, x, y) in actions:
                ax1.annotate(action,
                    xy=(x, y), xycoords='data',
                    # rotation=45,
                    xytext=(0.3, 0.75 - action_idx / 20), textcoords='figure fraction',
                    arrowprops=dict(facecolor='black', shrink=0.05, width=0.2),
                    horizontalalignment='left', verticalalignment='bottom',
                    fontsize=5)

                action_idx += 1
                if action_idx >= 10:
                    break


    box = ax1.get_position()
    ax1.set_position([box.x0, box.y0 + box.height * 0.1,
                     box.width, box.height * 0.9])

    # Put a legend below current axis
    #legend = ax1.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2),
    #          fancybox=False, ncol=3)
    ax1.set_ylim(bottom=0)
    ax1.legend(loc='upper right' if query != 1 else "lower center", ncol=stage_index, columnspacing=0.4, labelspacing=0.2)

    fig.tight_layout()
    #,legend2  
    fig.savefig("{}_{}_{}.pgf".format(prefix, query, settings_id),
        bbox_extra_artists=(), bbox_inches='tight')
    plt.close(fig)

def plot_code_cache(prefixes):
    rdf = []
    for prefix in prefixes:
        file1 = "{}/{}.csv".format(result_path, prefix)
        print("Reading file {}".format(file1))

        df = pd.read_csv(file1,
          sep='|', names=["hostname", "sha", "query_id",
            "cache_size", "threads", "reoptimize", "threads2",
            "total_comp_time_us", "total_comp_time_cyc",
            "total_wait_time_us", "total_wait_time_cyc",
            "gen_sign_cyc", "get_cy", "query_us"], header=None)
        rdf.append(df)

    _plot_code_cache(pd.concat(rdf), 'cache_1')

def _plot_code_cache(df, prefix):
    df.sort_values(by=['cache_size'])
    # df = df[df['threads'] == 1]
    queries = unique_values(df, "query_id")
    cache_sizes = unique_values(df, "cache_size")
    threads = unique_values(df, "threads")
    threads.sort()

    with open("{}.tex".format(prefix), "w+") as f:
        f.write("\\begin{tabular}{r ")
        for q in range(0, len(queries)*len(threads)):
            f.write("r ")
        f.write("}\n")
        f.write("\\toprule\n")

        f.write("Cache Size ")
        f.write("& \\multicolumn{" + str(len(threads)*len(queries)) + "}{c}{Runtime (s)}")
        f.write("\\\\\n")
        f.write("")
        #for t in threads:
        #  s = "Thread" if t == 1 else "Threads"
        #  f.write("& \\multicolumn{" + str(len(queries)) + "}{c}{" + str(t) + " " + s + "}")
        f.write("\\cmidrule(lr){2-" +str(len(threads)*len(queries)+1) + "}\n")
        index=2
        for t in threads:
          # f.write("\\cmidrule(lr){{{start}-{end}}}\n".format(start=index, end=1+index+len(queries)))
          index += len(queries)

        for t in threads:
            f.write(" & \\multicolumn{{{cols}}}{{c}}{{{threads}}}".format(cols=len(queries), threads="{} Threads".format(t) if t > 1 else "{} Thread".format(t)))
        f.write("\\\\\n")

        index=2
        for t in threads:
            z=len(queries)
            f.write("\\cmidrule(lr){{{s}-{e}}}".format(s=index, e=index+z-1))
            index+=z
        f.write("\n")
        for t in threads:
          for q in queries:
            f.write("& {} ".format(q.upper()))
        f.write("\\\\\n")
        f.write("\\midrule\n")

        baselines = {}
        for s in cache_sizes:
            f.write("{}".format(s))
            for num_t in threads:
              print(num_t)
              for q in queries:
                q_df = df[df['threads'] == num_t]
                q_df = q_df[q_df['cache_size'] == s]
                q_df = q_df[q_df['query_id'] == q]
                q_df = q_df[q_df['reoptimize'] == 0]

                assert(len(q_df) == 1)

                is_base = (s == 0) and (num_t == 1)

                for index, row in q_df.iterrows():
                    if is_base: 
                        baselines[q] = row["query_us"]
                    print(str(row["query_us"] / 1000.0))

                    t = "& {0:3.1f}".format(row["query_us"] / 1000000.0)

                    if not is_base:
                        t = t + " ($ {0:3.0f}\\times$)".format(
                            float(baselines[q])/float(row["query_us"]))
                    f.write(t)
                    break

            f.write("\\\\\n")
        f.write("\\bottomrule")
        f.write("\\end{tabular}")

def main():

    plot_code_cache(["cache_1", "cache_8"])

    for q in [1, 3, 4, 6, 9, 10, 12, 18]:
        mpl.rcParams.update({'font.size': 20})
        plot_speed("qspeed_1_sf50", q, 1)
        plot_speed("qspeed_1_sf50", q, 5)




    print("PLOT SEL")
    mpl.rcParams.update({'font.size': 20})

    # q6_prefix = "1_sf50"
    if True:
      q6_prefix = "all_sf300"
      plot_q6("q6_" + q6_prefix, giveYear=1992, giveQuant=1)
      plot_q6("q6_" + q6_prefix, giveYear=1999, giveQuant=1, legend=True)
      plot_q6("q6_" + q6_prefix, giveDisc=1, giveQuant=10)


    mpl.rcParams.update({'font.size': 15})

    plot_qperf("qperf_all_sf300", seconds=True)
    plot_qperf("qperf_all_sf50")


    plot_qperf("qperf_all_sf500", seconds=True)

    

    # plot_qperf("qperf_1_sf1")
    # plot_q1sel("q1sel_1_sf10")
    # plot_q1sel("sumsel_1_sf10")
    for learn in [False, True]:
        mpl.rcParams.update({'font.size': 22})
        # plot_qrisk("qrisk_1_sf10", "q9", learn)
        plot_qrisk("qrisk_1_sf10", "q1", learn)

        mpl.rcParams.update({'font.size': 15})
        # plot_adapt("adapt_1_sf10", "q9", learn, seconds=True)
        plot_adapt("adapt_1_sf10", "q1", learn, seconds=True)


    plot_qperf("qperf_1_sf1")
    plot_qperf("qperf_1_sf10")
    plot_qperf("qperf_1_sf10_100", errRange=False)
    plot_qperf("qperf_1_sf50")

    plot_qperf("join_1", sort=False, rotateX=45, filterPattern="join", moveLegend=1, seconds=True)
    plot_qperf("join_1", sort=False, rotateX=0, filterPattern="group", moveLegend=2, seconds=True)

    # plot_qperf("qperf_1_sf100")
    plot_qperf("qperf_all_sf100")
    


if __name__ == '__main__':
    main()
