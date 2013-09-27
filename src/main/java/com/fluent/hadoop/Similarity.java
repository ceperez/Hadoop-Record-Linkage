package com.fluent.hadoop;

import static com.fluent.hadoop.Distance.Distance;
import static com.google.common.primitives.Doubles.max;

public class Similarity
{
	public static final Similarity Similarity = new Similarity();

	public double bigram_dice(final char a[], final char b[])
	{
		if (a.length == 0 && b.length == 0)
			return 1;

		final char[] shorter = a.length < b.length ? a : b;
		final char[] longer = b.length > a.length ? b : a;
		int intersection = 0;

		for (int i = 0; i < shorter.length - 1; i++)
		{
			for (int k = 0; k < longer.length - 1; k++)
			{
				if (shorter[i] == longer[k] && shorter[i + 1] == longer[k + 1])
				{
					intersection++;
					break;
				}
			}
		}

		intersection += shorter[0] == longer[0] ? 1 : 0;
		intersection += shorter[shorter.length - 1] == longer[longer.length - 1] ? 1 : 0;

		return intersection / (longer.length + 1.);
	}

	public double bigram_dice(final Object a[], final Object b[])
	{
		if (a.length == 0 && b.length == 0)
			return 1;

		final Object[] shorter = a.length < b.length ? a : b;
		final Object[] longer = b.length > a.length ? b : a;
		int intersection = 0;

		for (int i = 0; i < shorter.length - 1; i++)
		{
			for (int k = 0; k < longer.length - 1; k++)
			{
				if (shorter[i].equals(longer[k]) && shorter[i + 1].equals(longer[k + 1]))
				{
					intersection++;
					break;
				}
			}
		}

		intersection += shorter[0].equals(longer[0]) ? 1 : 0;
		intersection += shorter[shorter.length - 1].equals(longer[longer.length - 1]) ? 1 : 0;

		return intersection / (longer.length + 1.);
	}

	public double bigram_dice(final String a, final String b)
	{
		return bigram_dice(a.toCharArray(), b.toCharArray());
	}

	public double normal_levenshtein(final String a, final String b)
	{
		return 1 - Distance.levenshtein(a.toCharArray(), b.toCharArray()) / max(a.length(), b.length(), 1);
	}

	static String normalise(final String a)
	{
		return a.trim().replace(' ', '_').toUpperCase();
	}
}
