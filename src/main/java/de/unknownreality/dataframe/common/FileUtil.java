/*
 *
 *  * Copyright (c) 2017 Alexander Grün
 *  *
 *  * Permission is hereby granted, free of charge, to any person obtaining a copy
 *  * of this software and associated documentation files (the "Software"), to deal
 *  * in the Software without restriction, including without limitation the rights
 *  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  * copies of the Software, and to permit persons to whom the Software is
 *  * furnished to do so, subject to the following conditions:
 *  *
 *  * The above copyright notice and this permission notice shall be included in all
 *  * copies or substantial portions of the Software.
 *  *
 *  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  * SOFTWARE.
 *
 */

package de.unknownreality.dataframe.common;

import java.io.File;
import java.io.FilenameFilter;
import java.util.regex.Pattern;

/**
 * Created by Alex on 01.04.2016.
 */
public class FileUtil {

    public static File[] findDirs(File parent, final String containedSubdir) {
        String[] dirNames = parent.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                File dir = new File(current, name);
                return dir.isDirectory() && new File(dir, containedSubdir).exists();
            }
        });

        File[] files = new File[dirNames.length];
        for (int i = 0; i < files.length; i++) {
            files[i] = new File(parent, dirNames[i]);
        }
        return files;
    }

    public static File[] findDirs(File parent, final Pattern namePattern) {
        String[] dirNames = parent.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                File dir = new File(current, name);
                return dir.isDirectory() && namePattern.matcher(name).matches();
            }
        });
        File[] files = new File[dirNames.length];
        for (int i = 0; i < files.length; i++) {
            files[i] = new File(parent, dirNames[i]);
        }
        return files;
    }

    public static File[] findDirs(File parent, final String containedSubdir, final Pattern namePattern) {
        String[] dirNames = parent.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                File dir = new File(current, name);
                return dir.isDirectory() && namePattern.matcher(name).matches() && new File(dir, containedSubdir).exists();
            }
        });
        File[] files = new File[dirNames.length];
        for (int i = 0; i < files.length; i++) {
            files[i] = new File(parent, dirNames[i]);
        }
        return files;
    }

    public static File[] findFiles(File parentDir, final Pattern namePattern) {
        String[] fileNames = parentDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                File file = new File(current, name);
                return file.isFile() && namePattern.matcher(name).matches();
            }
        });
        File[] files = new File[fileNames.length];
        for (int i = 0; i < files.length; i++) {
            files[i] = new File(parentDir, fileNames[i]);
        }
        return files;
    }

    public static File[] findFiles(File parentDir, final String... endings) {
        String[] fileNames = parentDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                File file = new File(current, name);
                return file.isFile() && acceptFile(file.getName(), endings);
            }
        });
        File[] files = new File[fileNames.length];
        for (int i = 0; i < files.length; i++) {
            files[i] = new File(parentDir, fileNames[i]);
        }
        return files;
    }


    public static boolean acceptFile(String fileName, String... fileEndings) {
        if (fileEndings == null || fileEndings.length == 0) {
            return true;
        }
        for (String fileEnding : fileEndings) {
            if (fileName.endsWith(fileEnding)) {
                return true;
            }
        }
        return false;
    }
}
