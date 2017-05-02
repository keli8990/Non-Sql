package com.mongodb.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

public class ComparatorDate implements Comparator {  
	  
    public int compare(Object obj1, Object obj2) {  
        Date begin = (Date) obj1;  
        Date end = (Date) obj2;  
        if (begin.after(end)) {  
            return 1;  
        } else {  
            return -1;  
        }  
    } 
}