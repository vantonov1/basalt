package com.github.vantonov1.basalt.repo.impl;

import java.util.Random;

public class GUID {
    private static final char[] CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();
    private static final Random GENERATOR = new Random();

    public static String generate() {
        char[] uuid = new char[36];
        long time=System.currentTimeMillis() >> 1;// makes it every 2 milliseconds
        final String prefix = Long.toHexString(time);
        assert prefix.length() >= 8;
        for (int i = 0; i < 8; i++) {
            uuid[i] = prefix.charAt(i);
        }


        // rfc4122 requires these characters
        uuid[8] = uuid[13] = uuid[18] = uuid[23] = '-';
        uuid[14] = '4';

        // Fill in random data.  At i==19 set the high bits of clock sequence as
        // per rfc4122, sec. 4.1.5
        for (int i = 8; i < 36; i++) {
            if (uuid[i] == 0) {
                int r = GENERATOR.nextInt(16);
                uuid[i] = CHARS[(i == 19) ? (r & 0x3) | 0x8 : r & 0xf];
            }
        }

        return new String(uuid);
    }

    public static boolean is(Object o) {
        try {
            if(!(o instanceof String)) {
                return false;
            }
            final String v = (String) o;
            if (v.length() != 36) return false;
            String[] components = v.split("-");
            if (components.length != 5)
                return false;
            for (int i = 0; i < 5; i++)
                components[i] = "0x" + components[i];

            long mostSigBits = Long.decode(components[0]);
            mostSigBits <<= 16;
            mostSigBits |= Long.decode(components[1]);
            mostSigBits <<= 16;
            mostSigBits |= Long.decode(components[2]);

            long leastSigBits = Long.decode(components[3]);
            leastSigBits <<= 48;
            leastSigBits |= Long.decode(components[4]);
            return mostSigBits != 0 && leastSigBits != 0;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }
}
