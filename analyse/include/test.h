#ifndef _TEST_H_
#define _TEST_H_

class Test
{
public:
	Test();
	~Test() {}

public:
	void Init();
	int GetVal() const;

private:
	int m_val;
};

#endif	// _TEST_H_

