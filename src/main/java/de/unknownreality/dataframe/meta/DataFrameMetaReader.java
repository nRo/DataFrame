/*
 * Copyright (c) 2016 Alexander Grün
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package de.unknownreality.dataframe.meta;

import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.common.ReaderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.util.*;

/**
 * Created by Alex on 07.06.2016.
 */
public class DataFrameMetaReader {
    private static Logger logger = LoggerFactory.getLogger(DataFrameMetaReader.class);

    /**
     * Map containing legacy package names.
     * This map is used to rename Reader and Column classes in an old Meta file.
     */
    private static final Map<String, String> LEGACY_PACKAGES = new TreeMap<>(new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            return -Integer.compare(o1.length(), o2.length());
        }
    });

    static {
        LEGACY_PACKAGES.put("de.unknownreality.data.", "de.unknownreality.dataframe.");
        LEGACY_PACKAGES.put("de.unknownreality.data.frame.", "de.unknownreality.dataframe.");
    }

    /**
     * Renames classes based in legacy packages
     *
     * @param className class name
     * @return class name in current packages
     */
    private static String remapLegacyPackages(String className) {
        for (String legacyPackage : LEGACY_PACKAGES.keySet()) {
            if (className.contains(legacyPackage)) {
                className = className.replace(legacyPackage, LEGACY_PACKAGES.get(legacyPackage));
                logger.warn("old package name found '{}'", legacyPackage);
            }
        }
        return className;
    }

    /**
     * Finds the reader builder class in a xml element
     *
     * @param readerBuilder reader builder xml element
     * @return reader builder class
     * @throws DataFrameMetaReaderException thrown if the reader builder class can not be found
     */
    private static Class<? extends ReaderBuilder> findReaderBuilderClass(Element readerBuilder) throws DataFrameMetaReaderException {
        String rbClassString = readerBuilder.getAttribute("class");
        if (rbClassString == null) {
            throw new DataFrameMetaReaderException("no readerBuilder class attribute found");
        }
        return parseChildClass(rbClassString, ReaderBuilder.class);
    }

    /**
     * Parses a class from a string and ensures that it is a child class of a specified parent class
     *
     * @param clName     child class name
     * @param parentType parent class
     * @param <T>        type of parent
     * @return child class
     * @throws DataFrameMetaReaderException thrown if the  class can not be found or is no child from the parent class
     */
    @SuppressWarnings("unchecked")
    private static <T> Class<? extends T> parseChildClass(String clName, Class<T> parentType) throws DataFrameMetaReaderException {
        clName = remapLegacyPackages(clName);
        Class<?> cl;
        try {
            cl = Class.forName(clName);
        } catch (ClassNotFoundException e) {
            throw new DataFrameMetaReaderException(String.format("class not found: %s", clName), e);
        }
        if (!parentType.isAssignableFrom(cl)) {
            throw new DataFrameMetaReaderException(String.format("%s does not extend %s",
                    clName, parentType.getCanonicalName()));
        }
        return (Class<? extends T>) cl;
    }

    /**
     * Extracts the attributes used to initialize the reader builder from a xml element
     *
     * @param readerBuilder xml element
     * @return attributes map
     * @throws DataFrameMetaReaderException thrown if the attributes can not be parsed
     */
    private static Map<String, String> findReaderBuilderAttributes(Element readerBuilder) throws DataFrameMetaReaderException {
        Map<String, String> readerBuilderAttributes = new HashMap<>();
        NodeList attributes = readerBuilder.getElementsByTagName("readerAttribute");
        for (int i = 0; i < attributes.getLength(); i++) {
            Node attributeNode = attributes.item(i);
            if (attributeNode.getNodeType() == Node.ELEMENT_NODE) {
                Element attribute = (Element) attributeNode;
                String name = attribute.getAttribute("name");
                if (name == null) {
                    throw new DataFrameMetaReaderException("no readerAttribute name attribute found");
                }
                String value = attribute.getAttribute("value");
                if (value == null) {
                    throw new DataFrameMetaReaderException("no readerAttribute value attribute found");
                }
                readerBuilderAttributes.put(name, value);
            } else {
                throw new DataFrameMetaReaderException("error parsing attributeElement element");
            }
        }
        return readerBuilderAttributes;
    }

    /**
     * Extracts the column information from a xml element
     *
     * @param columns columns xml element
     * @return columns map
     * @throws DataFrameMetaReaderException thrown of the columns can not be parsed
     */
    private static LinkedHashMap<String, Class<? extends DataFrameColumn>> findColumns(Element columns) throws DataFrameMetaReaderException {
        LinkedHashMap<String, Class<? extends DataFrameColumn>> columnsMap = new LinkedHashMap<>();
        NodeList columnNodes = columns.getElementsByTagName("column");
        for (int i = 0; i < columnNodes.getLength(); i++) {
            Node columnNode = columnNodes.item(i);
            if (columnNode.getNodeType() == Node.ELEMENT_NODE) {
                Element column = (Element) columnNode;
                String name = column.getAttribute("name");
                if (name == null) {
                    throw new DataFrameMetaReaderException("no column name attribute found");
                }
                String type = column.getAttribute("type");
                if (type == null) {
                    throw new DataFrameMetaReaderException("no column type attribute found");
                }
                Class<? extends DataFrameColumn> cl = parseChildClass(type, DataFrameColumn.class);
                columnsMap.put(name, cl);
            } else {
                throw new DataFrameMetaReaderException("error parsing column element");
            }
        }
        return columnsMap;
    }


    /**
     * Creates a data frame meta from an input file
     *
     * @param file input file
     * @return data frame meta
     * @throws DataFrameMetaReaderException thrown if the file can not be converted to a data frame meta
     */
    public static DataFrameMeta read(File file) throws DataFrameMetaReaderException {
        try {
            return read(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            throw new DataFrameMetaReaderException("error reading the meta file", e);
        }
    }

    /**
     * Creates a data frame meta from an input stream
     *
     * @param is input stream
     * @return data frame meta
     * @throws DataFrameMetaReaderException thrown if the input stream can not be converted to a data frame meta
     */
    public static DataFrameMeta read(InputStream is) throws DataFrameMetaReaderException {
        Document doc;
        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            doc = docBuilder.parse(is);
            doc.getDocumentElement().normalize();
        } catch (SAXException | ParserConfigurationException e) {
            throw new DataFrameMetaReaderException("error parsing dataFrameMeta file ", e);
        } catch (IOException e) {
            throw new DataFrameMetaReaderException("error reading dataFrameMeta file ", e);
        }
        Class<? extends ReaderBuilder> readerBuilderClass;
        Map<String, String> readerBuilderAttributes;
        LinkedHashMap<String, Class<? extends DataFrameColumn>> columns;

        NodeList readBuilderElements = doc.getElementsByTagName("readerBuilder");
        if (readBuilderElements.getLength() == 0) {
            throw new DataFrameMetaReaderException("no readerBuilder element found");
        }
        if (readBuilderElements.getLength() > 1) {
            throw new DataFrameMetaReaderException("multiple readerBuilder elements found");
        }

        Node readerBuilderNode = readBuilderElements.item(0);
        if (readerBuilderNode.getNodeType() == Node.ELEMENT_NODE) {
            Element readerBuilder = (Element) readerBuilderNode;
            readerBuilderClass = findReaderBuilderClass(readerBuilder);
            readerBuilderAttributes = findReaderBuilderAttributes(readerBuilder);
        } else {
            throw new DataFrameMetaReaderException("error parsing readerBuilder element");
        }

        NodeList columnsElements = doc.getElementsByTagName("columns");
        if (columnsElements.getLength() == 0) {
            throw new DataFrameMetaReaderException("no columns element found");
        }
        if (columnsElements.getLength() > 1) {
            throw new DataFrameMetaReaderException("multiple columns elements found");
        }
        Node columnsNode = columnsElements.item(0);
        if (columnsNode.getNodeType() == Node.ELEMENT_NODE) {
            Element columnsElement = (Element) columnsNode;
            columns = findColumns(columnsElement);
        } else {
            throw new DataFrameMetaReaderException("error parsing columns element");
        }
        return new DataFrameMeta(columns, readerBuilderClass, readerBuilderAttributes);

    }

    public static class DataFrameMetaReaderException extends Exception {
        public DataFrameMetaReaderException() {
        }

        public DataFrameMetaReaderException(String message) {
            super(message);
        }

        public DataFrameMetaReaderException(String message, Exception parent) {
            super(message, parent);
        }
    }
}
