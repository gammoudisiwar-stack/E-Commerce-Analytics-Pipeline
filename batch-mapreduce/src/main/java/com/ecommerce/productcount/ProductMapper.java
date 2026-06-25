package com.ecommerce.productcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ProductMapper — Job 1 (ProductCount)
 *
 * Lit chaque ligne du fichier de ventes (CSV) et émet la paire
 *   (product_id, quantity)
 *
 * Schéma CSV (index) :
 *   0=order_id 1=customer_id 2=product_id 3=category 4=price 5=discount
 *   6=quantity 7=payment_method 8=order_date ... 12=total_amount ...
 */
public class ProductMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text productId = new Text();
    private final IntWritable quantity = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Ignorer la ligne d'en-tête
        if (key.get() == 0 && line.startsWith("order_id")) {
            return;
        }

        String[] fields = line.split(",");
        // Vérification de robustesse : ligne complète attendue
        if (fields.length < 7) {
            return;
        }

        try {
            String pid = fields[2].trim();      // product_id
            int qty = Integer.parseInt(fields[6].trim()); // quantity

            if (pid.isEmpty()) {
                return;
            }

            productId.set(pid);
            quantity.set(qty);
            context.write(productId, quantity);
        } catch (NumberFormatException e) {
            // Ligne corrompue : on l'ignore (compteur Hadoop pour le suivi)
            context.getCounter("ProductCount", "MALFORMED_ROWS").increment(1);
        }
    }
}
