package com.ecommerce.categoryrevenue;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * CategoryRevenueReducer — Job 2 (CategoryRevenue)
 *
 * Additionne les montants (total_amount) pour chaque catégorie afin
 * d'obtenir le chiffre d'affaires total par catégorie de produit.
 *
 * Sortie : (product_category, chiffre_affaires_total)
 */
public class CategoryRevenueReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private final DoubleWritable result = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double sum = 0.0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }
        // Arrondi à 2 décimales pour un affichage propre du CA
        result.set(Math.round(sum * 100.0) / 100.0);
        context.write(key, result);
    }
}
