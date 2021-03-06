import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pandas.tools.plotting import scatter_matrix
import pyDOE

MPL = 12
NAME = 'samples_'+str(MPL)+'mpl'


def main():
    lhd = pyDOE.lhs(6, samples=100000, criterion=None) * MPL
    lhd_new = lhd[np.sum(lhd, axis=1) <= MPL+2]
    container = list()
    for element in lhd_new.flat:
        container.append(int(round(element, 0)))

    samples = np.array(container)
    (xdim, ydim) = lhd_new.shape
    samples = samples.reshape(xdim, ydim)
    samples = samples[np.sum(samples, axis=1) <= MPL]
    print(samples.shape)

    np.savetxt(NAME, samples, fmt='%d')
    # a = np.loadtxt('lhs', dtype='int')

    df = pd.DataFrame(samples, columns=['type 1', 'type 2', 'type 3',
                                        'type 4', 'type 5', 'type 6'])
    sm = scatter_matrix(df, alpha=0.2, figsize=(8, 8), diagonal='kde')

    plt.savefig(NAME+'.pdf', bbox_inches='tight')
    plt.show()


if __name__ == '__main__':
    main()
