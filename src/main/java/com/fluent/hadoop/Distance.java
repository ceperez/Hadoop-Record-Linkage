package com.fluent.hadoop;

import static com.google.common.primitives.Ints.min;

public class Distance
{
	public static final Distance Distance = new Distance();

	public int levenshtein(final char a[], final char b[])
	{
		final int A = a.length, B = b.length;
		final int scores[][] = new int[A + 1][B + 1];

		for (int i = 0; i <= A; i++)
			scores[i][0] = i;

		for (int k = 0; k <= B; k++)
			scores[0][k] = k;

		for (int k = 1; k <= B; k++)
		{
			for (int i = 1; i <= A; i++)
			{
				if (a[i - 1] == b[k - 1])
				{
					scores[i][k] = scores[i - 1][k - 1];
				}
				else
				{
					scores[i][k] = min(scores[i - 1][k] + 1, scores[i][k - 1] + 1, scores[i - 1][k - 1] + 1);
				}
			}
		}

		return scores[A][B];
	}
}
