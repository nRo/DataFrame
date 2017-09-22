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

package de.unknownreality.dataframe.meta;

import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataFrameException;
import de.unknownreality.dataframe.io.ReadFormat;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Alex on 07.06.2016.
 */
public class DataFrameMetaReader {
    private static Logger logger = LoggerFactory.getLogger(DataFrameMetaReader.class);

    /**
     * Map containing legacy package names.
     * This map is used to rename Reader and Column classes in an old Meta file.
     */
    private static final Map<String, String> LEGACY_PACKAGES = new TreeMap<>((o1, o2) -> -Integer.compare(o1.length(), o2.length()));
    static {
        LEGACY_PACKAGES.put("de.unknownreality.data.", "de.unknownreality.dataframe.");
        LEGACY_PACKAGES.put("de.unknownreality.data.frame.", "de.unknownreality.dataframe.");
        LEGACY_PACKAGES.put("de.unknownreality.data.csv.CSVReaderBuilder", "de.unknownreality.dataframe.csv.CSVFormat");
        LEGACY_PACKAGES.put("de.unknownreality.dataframe.csv.CSVReaderBuilder", "de.unknownreality.dataframe.csv.CSVFormat");
    }

    private DataFrameMetaReader(){}

    /**
     * Renames classes based in legacy packages
     *
     * @param className class name
     * @return class name in current packages
     */
    private static String remapLegacyPackages(String className) {
        String remappedClassname = className;
        for(Map.Entry<String,String> e : LEGACY_PACKAGES.entrySet()){
            if (remappedClassname.contains(e.getKey())) {
                remappedClassname = remappedClassname.replace(e.getKey(), e.getValue());
                logger.warn("old package name found '{}'", e.getKey());
            }
        }
        return remappedClassname;
    }

    /**
     * Finds the reader format class in a xml element
     *
     * @param readFormat reader format xml element
     * @return reader builder class
     * @throws DataFrameException thrown if the reader builder class can not be found
     */
    private static Class<? extends ReadFormat> findReadFormatClass(Element readFormat) throws DataFrameException {
        String rbClassString = readFormat.getAttribute("class");
        if (rbClassString == null) {
            throw new DataFrameException("no readFormat class attribute found");
        }
        return parseChildClass(rbClassString, ReadFormat.class);
    }

    /**
     * Parses a class from a string and ensures that it is a child class of a specified parent class
     *
     * @param clName     child class name
     * @param parentType parent class
     * @param <T>        type of parent
     * @return child class
     * @throws DataFrameException thrown if the  class can not be found or is no child from the parent class
     */
    @SuppressWarnings("unchecked")
    private static <T> Class<? extends T> parseChildClass(String clName, Class<T> parentType) throws DataFrameException {
        String convertedClassName = remapLegacyPackages(clName);
        Class<?> cl;
        try {
            cl = Class.forName(convertedClassName);
        } catch (ClassNotFoundException e) {
            throw new DataFrameException(String.format("class not found: %s", convertedClassName), e);
        }
        if (!parentType.isAssignableFrom(cl)) {
            throw new DataFrameException(String.format("%s does not extend %s",
                    convertedClassName, parentType.getCanonicalName()));
        }
        return (Class<? extends T>) cl;
    }

    /**
     * Extracts the attributes used to initialize the reader builder from a xml element
     *
     * @param readerBuilder xml element
     * @return attributes map
     * @throws DataFrameException thrown if the attributes can not be parsed
     */
    private static Map<String, String> findReaderBuilderAttributes(Element readerBuilder) throws DataFrameException {
        Map<String, String> readerBuilderAttributes = new HashMap<>();
        NodeList attributes = readerBuilder.getElementsByTagName("readerAttribute");
        for (int i = 0; i < attributes.getLength(); i++) {
            Node attributeNode = attributes.item(i);
            if (attributeNode.getNodeType() == Node.ELEMENT_NODE) {
                Element attribute = (Element) attributeNode;
                String name = attribute.getAttribute("name");
                if (name == null) {
                    throw new DataFrameException("no readerAttribute name attribute found");
                }
                String value = attribute.getAttribute("value");
                if (value == null) {
                    throw new DataFrameException("no readerAttribute value attribute found");
                }
                readerBuilderAttributes.put(name, value);
            } else {
                throw new DataFrameException("error parsing attributeElement element");
            }
        }
        return readerBuilderAttributes;
    }

    /**
     * Extracts the column information from a xml element
     *
     * @param columns columns xml element
     * @return columns map
     * @throws DataFrameException thrown of the columns can not be parsed
     */
    private static LinkedHashMap<String, Class<? extends DataFrameColumn>> findColumns(Element columns) throws DataFrameException {
        LinkedHashMap<String, Class<? extends DataFrameColumn>> columnsMap = new LinkedHashMap<>();
        NodeList columnNodes = columns.getElementsByTagName("column");
        for (int i = 0; i < columnNodes.getLength(); i++) {
            Node columnNode = columnNodes.item(i);
            if (columnNode.getNodeType() == Node.ELEMENT_NODE) {
                Element column = (Element) columnNode;
                String name = column.getAttribute("name");
                if (name == null) {
                    throw new DataFrameException("no column name attribute found");
                }
                String type = column.getAttribute("type");
                if (type == null) {
                    throw new DataFrameException("no column type attribute found");
                }
                Class<? extends DataFrameColumn> cl = parseChildClass(type, DataFrameColumn.class);
                columnsMap.put(name, cl);
            } else {
                throw new DataFrameException("error parsing column element");
            }
        }
        return columnsMap;
    }


    /**
     * Creates a data frame meta from an input file
     *
     * @param file input file
     * @return data frame meta
     * @throws DataFrameException thrown if the file can not be converted to a data frame meta
     */
    public static DataFrameMeta read(File file) throws DataFrameException {
        try {
            return read(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            throw new DataFrameException("error reading the meta file", e);
        }
    }

    /**
     * Creates a data frame meta from an input stream
     *
     * @param is input stream
     * @return data frame meta
     * @throws DataFrameException thrown if the input stream can not be converted to a data frame meta
     */
    public static DataFrameMeta read(InputStream is) throws DataFrameException {
        Document doc;
        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            doc = docBuilder.parse(is);
            doc.getDocumentElement().normalize();
        } catch (SAXException | ParserConfigurationException e) {
            throw new DataFrameException("error parsing dataFrameMeta file ", e);
        } catch (IOException e) {
            throw new DataFrameException("error reading dataFrameMeta file ", e);
        }
        Class<? extends ReadFormat> readFormatClass;
        Map<String, String> readerBuilderAttributes;
        LinkedHashMap<String, Class<? extends DataFrameColumn>> columns;

        NodeList readBuilderElements = doc.getElementsByTagName("readerBuilder");
        if (readBuilderElements.getLength() == 0) {
            throw new DataFrameException("no readerBuilder element found");
        }
        if (readBuilderElements.getLength() > 1) {
            throw new DataFrameException("multiple readerBuilder elements found");
        }

        Node readerBuilderNode = readBuilderElements.item(0);
        if (readerBuilderNode.getNodeType() == Node.ELEMENT_NODE) {
            Element readerBuilder = (Element) readerBuilderNode;
            readFormatClass = findReadFormatClass(readerBuilder);
            readerBuilderAttributes = findReaderBuilderAttributes(readerBuilder);
        } else {
            throw new DataFrameException("error parsing readerBuilder element");
        }

        NodeList columnsElements = doc.getElementsByTagName("columns");
        if (columnsElements.getLength() == 0) {
            throw new DataFrameException("no columns element found");
        }
        if (columnsElements.getLength() > 1) {
            throw new DataFrameException("multiple columns elements found");
        }
        Node columnsNode = columnsElements.item(0);
        if (columnsNode.getNodeType() == Node.ELEMENT_NODE) {
            Element columnsElement = (Element) columnsNode;
            columns = findColumns(columnsElement);
        } else {
            throw new DataFrameException("error parsing columns element");
        }
        return new DataFrameMeta(columns, readFormatClass, readerBuilderAttributes);

    }

}
