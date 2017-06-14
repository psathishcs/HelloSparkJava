package org.hello.entity;

import java.io.Serializable;
import java.sql.Date;

/**
 * 
 * @author SathishParthasarathy Jun 13, 2017 10:04:08 PM
 */
public class Employees implements Serializable {

	private static final long serialVersionUID = 1L;
	Long emp_no;
	Date birth_date;
	String first_name;
	String last_name;
	String gender;
	Date hire_date;

	public Long getEmp_no() {
		return emp_no;
	}

	public void setEmp_no(Long emp_no) {
		this.emp_no = emp_no;
	}

	public String getFirst_name() {
		return first_name;
	}

	public void setFirst_name(String first_name) {
		this.first_name = first_name;
	}

	public String getLast_name() {
		return last_name;
	}

	public void setLast_name(String last_name) {
		this.last_name = last_name;
	}
	
	public Date getBirth_date() {
		return birth_date;
	}

	public void setBirth_date(Date birth_date) {
		this.birth_date = birth_date;
	}

	public Date getHire_date() {
		return hire_date;
	}

	public void setHire_date(Date hire_date) {
		this.hire_date = hire_date;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

}
