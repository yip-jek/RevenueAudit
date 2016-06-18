#pragma once

#include <math.h>

// 基础比较类
class ThresholdCompare
{
public:
	ThresholdCompare(const double& threshold): m_threshold(threshold)
	{}
	virtual ~ThresholdCompare() {}

public:
	virtual bool IsReachThreshold(const double& tg, const double& src) = 0;

protected:
	virtual double CompareRatio(const double& tg, const double& src)
	{ return (fabs(tg - src) / src); }

protected:
	double m_threshold;
};

// 比较类：大于
class G_ThresholdCompare : public ThresholdCompare
{
public:
	G_ThresholdCompare(const double& threshold): ThresholdCompare(threshold)
	{}
	virtual ~G_ThresholdCompare() {}

public:
	virtual bool IsReachThreshold(const double& tg, const double& src)
	{ return (CompareRatio(tg, src) > m_threshold); }
};

// 比较类：大于或等于
class GE_ThresholdCompare : public ThresholdCompare
{
public:
	GE_ThresholdCompare(const double& threshold): ThresholdCompare(threshold)
	{}
	virtual ~GE_ThresholdCompare() {}

public:
	virtual bool IsReachThreshold(const double& tg, const double& src)
	{ return (CompareRatio(tg, src) >= m_threshold); }
};

