/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.demos.dimensions.ads;

import java.util.Map;

/**
 *
 * @author rcongiu
 */
 /**
     * We introduce this to carry mappings and other options
     * we may want to support in the future.
     * Signature for caching will be built using this.
     */
public  class ByteArrayStructOIOptions {
        Map<String,String> mappings;
        public ByteArrayStructOIOptions (Map<String,String> mp) {
            mappings = mp;
        }

        public Map<String, String> getMappings() {
            return mappings;
        }



        @Override
        public boolean equals(Object obj) {
            if(obj == null || !(obj instanceof ByteArrayStructOIOptions) ) {
                return false ;
            } else {
                ByteArrayStructOIOptions oio = (ByteArrayStructOIOptions) obj;

                if(mappings != null) {
                    return mappings.equals(oio.mappings);
                } else {
                    return mappings == oio.mappings;
                }
            }
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 67 * hash + (this.mappings != null ? this.mappings.hashCode() : 0);
            return hash;
        }


    }