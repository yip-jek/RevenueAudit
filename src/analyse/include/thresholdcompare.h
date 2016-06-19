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
	// 判断是否达到阈值，并返回阈值的计算结果
	virtual bool ReachThreshold(const double& tg, const double& src, double* p_th) = 0;

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
	// 判断是否达到阈值，并返回阈值的计算结果
	virtual bool ReachThreshold(const double& tg, const double& src, double* p_th)
	{
		double dou_th = CompareRatio(tg, src);
		if ( p_th != NULL )
		{
			*p_th = dou_th;
		}
		return (dou_th > m_threshold);
	}
};

// 比较类：大于或等于
class GE_ThresholdCompare : public ThresholdCompare
{
public:
	GE_ThresholdCompare(const double& threshold): ThresholdCompare(threshold)
	{}
	virtual ~GE_ThresholdCompare() {}

public:
	// 判断是否达到阈值，并返回阈值的计算结果
	virtual bool ReachThreshold(const double& tg, const double& src, double* p_th)
	{
		double dou_th = CompareRatio(tg, src);
		if ( p_th != NULL )
		{
			*p_th = dou_th;
		}
		return (dou_th >= m_threshold);
	}
};

